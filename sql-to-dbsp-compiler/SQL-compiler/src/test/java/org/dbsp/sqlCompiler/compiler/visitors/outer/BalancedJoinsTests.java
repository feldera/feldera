package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.junit.Assert;
import org.junit.Test;

/** {@link BalancedJoins} marks the joins DBSP can balance; the {@code FELDERA_ADAPTIVE_JOINS}
 * setting is reflected as a circuit property and decides whether they run adaptively. */
public class BalancedJoinsTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        // Only an incremental join can be balanced; a non-incremental circuit uses stream joins.
        options.languageOptions.incrementalize = true;
        return options;
    }

    /** Counts the joins of a circuit and how many of them are marked balanced. */
    static class JoinCounter extends CircuitVisitor {
        int joins = 0;
        int balanced = 0;
        /** Persistent id of the last join visited. */
        String persistentId = "";

        JoinCounter(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPJoinBaseOperator join) {
            this.joins++;
            if (join.balanced)
                this.balanced++;
            this.persistentId = OperatorHash.getHash(join, true).toString();
        }
    }

    static final String ON = "SET FELDERA_ADAPTIVE_JOINS = ON;\n";
    static final String TABLES = """
            CREATE TABLE T(x INT NOT NULL, y INT);
            CREATE TABLE S(x INT NOT NULL, z INT);
            """;
    static final String JOIN = "CREATE VIEW V AS SELECT T.x, T.y, S.z FROM T JOIN S ON T.x = S.x;";
    static final String HINTED_JOIN =
            "CREATE VIEW V AS SELECT /*+ broadcast(S) */ T.x, T.y, S.z FROM T JOIN S ON T.x = S.x;";

    JoinCounter countJoins(CompilerCircuit cc) {
        JoinCounter counter = new JoinCounter(cc.compiler);
        cc.visit(counter);
        return counter;
    }

    /** The balanced flag records eligibility; the setting does not change it. */
    @Test
    public void flagMeansEligible() {
        for (String prefix: new String[]{"", ON, "SET FELDERA_ADAPTIVE_JOINS = OFF;\n"}) {
            JoinCounter counter = this.countJoins(this.getCC(prefix + TABLES + JOIN));
            Assert.assertEquals(1, counter.joins);
            Assert.assertEquals(1, counter.balanced);
        }
    }

    /** The setting is a circuit property, not part of any operator, so persistent ids stay put. */
    @Test
    public void settingKeepsPersistentId() {
        JoinCounter plain = this.countJoins(this.getCC(TABLES + JOIN));
        JoinCounter adaptive = this.countJoins(this.getCC(ON + TABLES + JOIN));
        Assert.assertEquals(1, plain.joins);
        Assert.assertEquals(plain.persistentId, adaptive.persistentId);
    }

    /** The generated circuit sets the property first thing and still calls the balanced join. */
    @Test
    public void settingIsEmitted() {
        String off = this.getCC(TABLES + JOIN).getRustSources();
        Assert.assertTrue(off.contains("circuit.set_adaptive_joins(false);"));
        Assert.assertTrue(off.contains("join_balanced("));
        String on = this.getCC(ON + TABLES + JOIN).getRustSources();
        Assert.assertTrue(on.contains("circuit.set_adaptive_joins(true);"));
        Assert.assertTrue(on.contains("join_balanced("));
    }

    /** The SET variables survive a trip through the JSON IR. */
    @Test
    public void settingsRoundTripThroughJson() {
        CompilerCircuit cc = this.getCC(
                ON + "SET FELDERA_WINDOW_SHARING_THRESHOLD = 4;\n" + TABLES + JOIN);
        DBSPCircuit decoded = new TestSerialize(cc.compiler).apply(cc.getCircuit());
        Assert.assertNotSame(cc.getCircuit().metadata, decoded.metadata);
        Assert.assertTrue(decoded.metadata.adaptiveJoins());
        Assert.assertEquals(4, decoded.metadata.windowSharingThreshold());

        cc = this.getCC(TABLES + JOIN);
        decoded = new TestSerialize(cc.compiler).apply(cc.getCircuit());
        Assert.assertFalse(decoded.metadata.adaptiveJoins());
    }

    @Test
    public void hintEmittedWhenOn() {
        CompilerCircuit cc = this.getCC(ON + TABLES + HINTED_JOIN);
        Assert.assertEquals(0, cc.compiler.messages.warningCount());
        Assert.assertTrue(cc.getRustSources().contains("circuit.set_balancer_hint("));
    }

    /** A hint requires an adaptive join; with the setting off the compiler
     * reports it and the runtime ignores the hint it still emits. */
    @Test
    public void hintIgnoredWhenOff() {
        CompilerCircuit cc = this.getCC(TABLES + HINTED_JOIN);
        Assert.assertEquals(1, cc.compiler.messages.warningCount());
        // A quiet compiler leaves warnings out of messages.toString(); format the message itself.
        String warning = cc.compiler.messages.messages.get(0).toString();
        Assert.assertTrue(warning, warning.contains("Hint Broadcast(s) cannot be implemented: " +
                "adaptive joins are off; enable them with SET FELDERA_ADAPTIVE_JOINS = ON"));
        String rust = cc.getRustSources();
        Assert.assertTrue(rust.contains("circuit.set_adaptive_joins(false);"));
        Assert.assertTrue(rust.contains("circuit.set_balancer_hint("));
    }

    /** DBSP cannot balance a join inside a recursive view; a hint there is reported and dropped. */
    @Test
    public void hintInsideRecursionIsIgnored() {
        CompilerCircuit cc = this.getCC(ON + """
                CREATE TABLE E(s INT NOT NULL, t INT NOT NULL);
                DECLARE RECURSIVE VIEW R(s INT NOT NULL, t INT NOT NULL);
                CREATE VIEW R AS
                    SELECT * FROM E
                    UNION
                    SELECT /*+ broadcast(E) */ E.s, R.t FROM E JOIN R ON E.t = R.s;""");
        JoinCounter counter = this.countJoins(cc);
        Assert.assertEquals(1, counter.joins);
        Assert.assertEquals(0, counter.balanced);
        Assert.assertEquals(1, cc.compiler.messages.warningCount());
        String warning = cc.compiler.messages.messages.get(0).toString();
        Assert.assertTrue(warning, warning.contains(
                "Hint Broadcast(e) cannot be implemented: the join is inside a recursive view"));
        Assert.assertFalse(cc.getRustSources().contains("set_balancer_hint"));
    }

    static final String STEP_INPUT = """
            INSERT INTO T VALUES (1, 10), (2, 20);
            INSERT INTO S VALUES (1, 100), (3, 300);""";
    static final String STEP_OUTPUT = """
             x | y  | z   | weight
            ----------------------
             1 | 10 | 100 | 1""";

    /** With the setting on, the three-worker test circuit runs the hinted join adaptively. */
    @Test
    public void adaptiveJoinRuns() {
        var ccs = this.getCCS(ON + TABLES + HINTED_JOIN);
        ccs.step(STEP_INPUT, STEP_OUTPUT);
    }

    /** With the setting off, the runtime runs the balanced join as a plain hash join. */
    @Test
    public void plainJoinRuns() {
        var ccs = this.getCCS(TABLES + JOIN);
        ccs.step(STEP_INPUT, STEP_OUTPUT);
    }

    /** A hint the runtime ignores is still emitted, so setting it must not fail the circuit. */
    @Test
    public void hintedPlainJoinRuns() {
        var ccs = this.getCCS(TABLES + HINTED_JOIN);
        ccs.step(STEP_INPUT, STEP_OUTPUT);
    }
}
