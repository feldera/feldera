package org.dbsp.sqlCompiler.compiler.sql.mysql;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

// https://github.com/mysql/mysql-server/tree/trunk/mysql-test/r/varbinary.result
public class VarbinaryTests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        // Replaced hex numbers with VARBINARY literals
        String sql = "create TABLE t1(a INT, b VARBINARY(4), c VARBINARY(4));\n" +
                "INSERT INTO t1 VALUES\n" +
                "(1, x'31393831', x'31303037'),\n" +
                "(2, x'31393832', x'31303038'),\n" +
                "(3, x'31393833', x'31303039'),\n" +
                "(3, x'31393834', x'31393831'),\n" +
                "(4, x'31393835', x'31393832'),\n" +
                "(5, x'31393836', x'31303038');";
        compiler.compileStatements(sql);
    }

    @Test @Ignore("Calcite bug https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6095")
    public void testVarBinary() {
        this.q("select x'31', X'ffff'+0;\n" +
                "x'31'\tX'ffff'+0\n" +
                "1\t65535");
    }

    @Test
    public void testWrongBinary() {
        this.shouldFail("select x'hello'",
                "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
    }

    @Test @Ignore("Calcite does not yet support bitwise functions https://issues.apache.org/jira/browse/CALCITE-5087")
    public void testHex() {
        this.q("SELECT\n" +
                "hex(BITAND(b, c)), hex(BITAND(b, 0x31393838)), BITAND(b, NULL), hex(BITAND(b, 0b00000000000000000000000000001011)),\n" +
                "hex(BITAND(0x31393838, b)), BITAND(NULL, b), hex(BITAND(0b00000000000000000000000000001011, b))\n" +
                "FROM t1;\n" +
                "hex(b & c)\thex(b & 0x31393838)\tb & NULL\thex(b & 0b00000000000000000000000000001011)\thex(0x31393838 & b)\tNULL & b\thex(0b00000000000000000000000000001011 & b)\n" +
                "31303031\t31393830\tNULL\t00000001\t31393830\tNULL\t00000001\n" +
                "31303030\t31393830\tNULL\t00000002\t31393830\tNULL\t00000002\n" +
                "31303031\t31393830\tNULL\t00000003\t31393830\tNULL\t00000003\n" +
                "31393830\t31393830\tNULL\t00000000\t31393830\tNULL\t00000000\n" +
                "31393830\t31393830\tNULL\t00000001\t31393830\tNULL\t00000001\n" +
                "31303030\t31393830\tNULL\t00000002\t31393830\tNULL\t00000002\n" +
                "\n" +
                "SELECT\n" +
                "hex(b | c), hex(b | 0x31393838), b | NULL, hex(b | 0b00000000000000000000000000001011),\n" +
                "hex(0x31393838 | b), NULL | b, hex(0b00000000000000000000000000001011 | b)\n" +
                "FROM t1;\n" +
                "hex(b | c)\thex(b | 0x31393838)\tb | NULL\thex(b | 0b00000000000000000000000000001011)\thex(0x31393838 | b)\tNULL | b\thex(0b00000000000000000000000000001011 | b)\n" +
                "31393837\t31393839\tNULL\t3139383B\t31393839\tNULL\t3139383B\n" +
                "3139383A\t3139383A\tNULL\t3139383B\t3139383A\tNULL\t3139383B\n" +
                "3139383B\t3139383B\tNULL\t3139383B\t3139383B\tNULL\t3139383B\n" +
                "31393835\t3139383C\tNULL\t3139383F\t3139383C\tNULL\t3139383F\n" +
                "31393837\t3139383D\tNULL\t3139383F\t3139383D\tNULL\t3139383F\n" +
                "3139383E\t3139383E\tNULL\t3139383F\t3139383E\tNULL\t3139383F\n" +
                "\n" +
                "SELECT\n" +
                "hex(b ^ c), hex(b ^ 0x31393838), b ^ NULL, hex(b ^ 0b00000000000000000000000000001011),\n" +
                "hex(0x31393838 ^ b), NULL ^ b, hex(0b00000000000000000000000000001011 ^ b)\n" +
                "FROM t1;\n" +
                "hex(b ^ c)\thex(b ^ 0x31393838)\tb ^ NULL\thex(b ^ 0b00000000000000000000000000001011)\thex(0x31393838 ^ b)\tNULL ^ b\thex(0b00000000000000000000000000001011 ^ b)\n" +
                "00090806\t00000009\tNULL\t3139383A\t00000009\tNULL\t3139383A\n" +
                "0009080A\t0000000A\tNULL\t31393839\t0000000A\tNULL\t31393839\n" +
                "0009080A\t0000000B\tNULL\t31393838\t0000000B\tNULL\t31393838\n" +
                "00000005\t0000000C\tNULL\t3139383F\t0000000C\tNULL\t3139383F\n" +
                "00000007\t0000000D\tNULL\t3139383E\t0000000D\tNULL\t3139383E\n" +
                "0009080E\t0000000E\tNULL\t3139383D\t0000000E\tNULL\t3139383D\n" +
                "\n" +
                "SELECT BIT_COUNT(b), HEX(~b), HEX(b << 1), HEX(b >> 1) from t1;\n" +
                "BIT_COUNT(b)\tHEX(~b)\tHEX(b << 1)\tHEX(b >> 1)\n" +
                "13\tCEC6C7CE\t62727062\t189C9C18\n" +
                "13\tCEC6C7CD\t62727064\t189C9C19\n" +
                "14\tCEC6C7CC\t62727066\t189C9C19\n" +
                "13\tCEC6C7CB\t62727068\t189C9C1A\n" +
                "14\tCEC6C7CA\t6272706A\t189C9C1A\n" +
                "14\tCEC6C7C9\t6272706C\t189C9C1B\n" +
                "\n" +
                "SELECT HEX(BIT_AND(b)), HEX(BIT_OR(b)), HEX(BIT_XOR(b)) FROM t1;\n" +
                "HEX(BIT_AND(b))\tHEX(BIT_OR(b))\tHEX(BIT_XOR(b))\n" +
                "31393830\t31393837\t00000007");
    }

    @Test
    public void testBitAndAgg() {
        // Changed HEX to TO_HEX
        this.q("SELECT TO_HEX(BIT_AND(b)), TO_HEX(BIT_OR(b)), TO_HEX(BIT_XOR(b)) FROM t1 GROUP BY a;\n" +
                " HEX(BIT_AND(b))\t HEX(BIT_OR(b))\t HEX(BIT_XOR(b))\n" +
                " 31393831\t 31393831\t 31393831\n" +
                " 31393832\t 31393832\t 31393832\n" +
                " 31393830\t 31393837\t 00000007\n" +
                " 31393835\t 31393835\t 31393835\n" +
                " 31393836\t 31393836\t 31393836");
    }

    @Test
    public void testConcatBinary() {
        this.q("SELECT x'0a' || x'bc';\n" +
                "result\n" +
                "------\n" +
                " 0abc");
    }
}
