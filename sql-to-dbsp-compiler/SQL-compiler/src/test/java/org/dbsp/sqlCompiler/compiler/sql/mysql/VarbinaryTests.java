package org.dbsp.sqlCompiler.compiler.sql.mysql;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

// https://github.com/mysql/mysql-server/tree/trunk/mysql-test/r/varbinary.result
public class VarbinaryTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // Replaced hex numbers with VARBINARY literals
        String sql = """
                create TABLE t1(a INT, b VARBINARY(4), c VARBINARY(4));
                INSERT INTO t1 VALUES
                (1, x'31393831', x'31303037'),
                (2, x'31393832', x'31303038'),
                (3, x'31393833', x'31303039'),
                (3, x'31393834', x'31393831'),
                (4, x'31393835', x'31393832'),
                (5, x'31393836', x'31303038');""";
        compiler.submitStatementsForCompilation(sql);
    }

    @Test
    public void testCastInt() {
        this.queryFailingInCompilation("select CAST(X'ffff' AS INT)",
                "Cast function cannot convert value of type BINARY(2) NOT NULL to type INTEGER");
    }

    @Test
    public void testVarBinary() {
        this.queryFailingInCompilation("select X'ffff'+0", "Cannot apply '+' to arguments of type");
    }

    @Test
    public void testWrongBinary() {
        this.queryFailingInCompilation("select x'hello'",
                "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
    }

    @Test @Ignore("HEX and binary functions not yet implemented")
    public void testHex() {
        this.q("""
                SELECT
                hex(BITAND(b, c)), hex(BITAND(b, 0x31393838)), BITAND(b, NULL), hex(BITAND(b, 0b00000000000000000000000000001011)),
                hex(BITAND(0x31393838, b)), BITAND(NULL, b), hex(BITAND(0b00000000000000000000000000001011, b))
                FROM t1;
                hex(b & c)\thex(b & 0x31393838)\tb & NULL\thex(b & 0b00000000000000000000000000001011)\thex(0x31393838 & b)\tNULL & b\thex(0b00000000000000000000000000001011 & b)
                31303031\t31393830\tNULL\t00000001\t31393830\tNULL\t00000001
                31303030\t31393830\tNULL\t00000002\t31393830\tNULL\t00000002
                31303031\t31393830\tNULL\t00000003\t31393830\tNULL\t00000003
                31393830\t31393830\tNULL\t00000000\t31393830\tNULL\t00000000
                31393830\t31393830\tNULL\t00000001\t31393830\tNULL\t00000001
                31303030\t31393830\tNULL\t00000002\t31393830\tNULL\t00000002

                SELECT
                hex(b | c), hex(b | 0x31393838), b | NULL, hex(b | 0b00000000000000000000000000001011),
                hex(0x31393838 | b), NULL | b, hex(0b00000000000000000000000000001011 | b)
                FROM t1;
                hex(b | c)\thex(b | 0x31393838)\tb | NULL\thex(b | 0b00000000000000000000000000001011)\thex(0x31393838 | b)\tNULL | b\thex(0b00000000000000000000000000001011 | b)
                31393837\t31393839\tNULL\t3139383B\t31393839\tNULL\t3139383B
                3139383A\t3139383A\tNULL\t3139383B\t3139383A\tNULL\t3139383B
                3139383B\t3139383B\tNULL\t3139383B\t3139383B\tNULL\t3139383B
                31393835\t3139383C\tNULL\t3139383F\t3139383C\tNULL\t3139383F
                31393837\t3139383D\tNULL\t3139383F\t3139383D\tNULL\t3139383F
                3139383E\t3139383E\tNULL\t3139383F\t3139383E\tNULL\t3139383F

                SELECT
                hex(b ^ c), hex(b ^ 0x31393838), b ^ NULL, hex(b ^ 0b00000000000000000000000000001011),
                hex(0x31393838 ^ b), NULL ^ b, hex(0b00000000000000000000000000001011 ^ b)
                FROM t1;
                hex(b ^ c)\thex(b ^ 0x31393838)\tb ^ NULL\thex(b ^ 0b00000000000000000000000000001011)\thex(0x31393838 ^ b)\tNULL ^ b\thex(0b00000000000000000000000000001011 ^ b)
                00090806\t00000009\tNULL\t3139383A\t00000009\tNULL\t3139383A
                0009080A\t0000000A\tNULL\t31393839\t0000000A\tNULL\t31393839
                0009080A\t0000000B\tNULL\t31393838\t0000000B\tNULL\t31393838
                00000005\t0000000C\tNULL\t3139383F\t0000000C\tNULL\t3139383F
                00000007\t0000000D\tNULL\t3139383E\t0000000D\tNULL\t3139383E
                0009080E\t0000000E\tNULL\t3139383D\t0000000E\tNULL\t3139383D

                SELECT BIT_COUNT(b), HEX(~b), HEX(b << 1), HEX(b >> 1) from t1;
                BIT_COUNT(b)\tHEX(~b)\tHEX(b << 1)\tHEX(b >> 1)
                13\tCEC6C7CE\t62727062\t189C9C18
                13\tCEC6C7CD\t62727064\t189C9C19
                14\tCEC6C7CC\t62727066\t189C9C19
                13\tCEC6C7CB\t62727068\t189C9C1A
                14\tCEC6C7CA\t6272706A\t189C9C1A
                14\tCEC6C7C9\t6272706C\t189C9C1B

                SELECT HEX(BIT_AND(b)), HEX(BIT_OR(b)), HEX(BIT_XOR(b)) FROM t1;
                HEX(BIT_AND(b))\tHEX(BIT_OR(b))\tHEX(BIT_XOR(b))
                31393830\t31393837\t00000007""");
    }

    @Test
    public void testBitAndAgg() {
        // Changed HEX to TO_HEX
        this.q("""
                SELECT TO_HEX(BIT_AND(b)), TO_HEX(BIT_OR(b)), TO_HEX(BIT_XOR(b)) FROM t1 GROUP BY a;
                 HEX(BIT_AND(b))\t HEX(BIT_OR(b))\t HEX(BIT_XOR(b))
                 31393831\t 31393831\t 31393831
                 31393832\t 31393832\t 31393832
                 31393830\t 31393837\t 00000007
                 31393835\t 31393835\t 31393835
                 31393836\t 31393836\t 31393836""");
    }

    @Test
    public void testToInt() {
        this.q("""
                SELECT TO_INT(x'0a'), TO_INT(x'1bcdef00'), TO_INT(x'1bcdef000506'), TO_INT(x'0abc');
                 1 | 2 | 3 | 4
                ----------------
                 10 | 466480896 | 466480896 | 2748""");
    }

    @Test
    public void testConcatBinary() {
        this.qs("""
                SELECT x'0a' || x'bc';
                result
                ------
                 0abc
                (1 row)
                
                SELECT x'0A' || NULL;
                result
                ------
                NULL
                (1 row)
                
                SELECT b || c FROM t1;
                result
                ------
                 3139383131303037
                 3139383231303038
                 3139383331303039
                 3139383431393831
                 3139383531393832
                 3139383631303038
                (6 rows)""");
    }

    // Tested on Postgres
    // While Postgres doesn't allow negative "overlay from" value, we treat it as 0
    @Test
    public void testOverlay() {
        this.qs("""
                SELECT overlay(x'1234567890'::bytea placing x'0203' from 2 for 3);
                 overlay
                ---------
                 12020390
                (1 row)

                SELECT overlay(x'123456'::bytea placing x'1010' from 1 for 0);
                 overlay
                ---------
                 1010123456
                (1 row)

                SELECT overlay(x'123456'::bytea placing x'1010' from 1 for 1);
                 overlay
                ---------
                 10103456
                (1 row)

                SELECT overlay(x'123456'::bytea placing x'1010' from 1 for -1);
                 overlay
                ---------
                 1010123456
                (1 row)

                SELECT overlay(x'123456'::bytea placing x'1010' from -1);
                 overlay
                ---------
                 123456
                (1 row)

                SELECT overlay(x'123456'::bytea placing x'7890' from 7);
                 overlay
                ---------
                 1234567890
                (1 row)"""
        );
    }

    @Test
    public void testCast() {
        this.qs("""
                SELECT CAST('abcd1234' AS VARBINARY);
                 r
                ---
                 6162636431323334
                (1 row)

                SELECT CAST(x'abcd1234' AS BINARY(2));
                 r
                ---
                 abcd
                (1 row)
                
                SELECT CAST(x'abcd' AS BINARY(4));
                 r
                ---
                 abcd0000
                (1 row)""");
    }

    @Test
    public void testLeftAndRight() {
        this.qs("""
                SELECT right(x'ABCdef', 1);
                result
                -------
                 ef
                (1 row)
                
                SELECT right(x'ABCdef', 0);
                result
                -------
                 \s
                (1 row)
                
                SELECT right(x'ABCdef', 4);
                result
                -------
                 abcdef
                (1 row)
                
                SELECT right(x'ABCdef', -2);
                result
                -------
                 \s
                (1 row)
                
                SELECT right(cast(null as binary(1)), -2);
                result
                -------
                NULL
                (1 row)
                
                SELECT left(x'ABCdef', 1);
                result
                -------
                 ab
                (1 row)
                
                SELECT left(x'ABCdef', 0);
                result
                -------
                 \s
                (1 row)
                
                SELECT left(x'ABCdef', 4);
                result
                -------
                 abcdef
                (1 row)
                
                SELECT left(x'ABCdef', -2);
                result
                -------
                 \s
                (1 row)
                
                SELECT left(cast(null as binary(1)), -2);
                result
                -------
                NULL
                (1 row)
                
                SELECT left(x'ABCdef', cast(null as Integer));
                result
                -------
                NULL
                (1 row)""");
    }

    // Tested on Postgres
    @Test
    public void testSubstring() {
        this.qs("""
                SELECT substring(x'123456', 0);
                 substring
                -----------
                 123456
                (1 row)

                SELECT substring(x'123456', 1);
                 substring
                -----------
                 123456
                (1 row)

                SELECT substring(x'123456', 3);
                 substring
                -----------
                 56
                (1 row)

                SELECT substring(x'123456', -1);
                 substring
                -----------
                 123456
                (1 row)

                SELECT substring(x'1234567890', 3, 2);
                 substring
                -----------
                 5678
                (1 row)

                SELECT substring(x'123456'::bytea from -2 for 6);
                 substring
                -----------
                 123456
                (1 row)""");
    }

    @Test
    public void testSubstringFail() {
        this.qs("""
                SELECT substring(x'123456', 3, -1);
                result
                ------
                 \s
                (1 row)""");
    }

    @Test
    public void testOctetLength() {
        this.qs("""
                SELECT octet_length(x'123456'::bytea);
                 length
                --------
                 3
                (1 row)

                SELECT octet_length(x'10102323'::bytea);
                 length
                --------
                 4
                (1 row)

                SELECT octet_length(x''::bytea);
                 length
                --------
                 0
                (1 row)

                SELECT octet_length(x'0abc'::bytea);
                 length
                --------
                 2
                (1 row)
                """
        );
    }

    @Test
    public void testPosition() {
        this.qs("""
                SELECT position(x'20' IN x'102023'::bytea);
                 position
                ----------
                 2
                (1 row)
                
                SELECT position(x'24' IN x'102023'::bytea);
                 position
                ----------
                 0
                (1 row)"""
        );
    }
}
