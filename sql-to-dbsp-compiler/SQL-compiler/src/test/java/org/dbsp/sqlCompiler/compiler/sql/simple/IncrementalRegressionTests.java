package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

/** Regression tests that fail in incremental mode */
public class IncrementalRegressionTests extends SqlIoTest {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(true, true);
        return new DBSPCompiler(options);
    }

    @Test
    public void issue2018() {
        String sql = """
                CREATE TABLE customer (
                    c_id INT,
                    c_d_id INT,
                    c_w_id INT,
                    c_first VARCHAR(16),
                    c_middle CHAR(2),
                    c_last VARCHAR(16),
                    c_street_1 VARCHAR(20),
                    c_street_2 VARCHAR(20),
                    c_city VARCHAR(20),
                    c_state CHAR(2),
                    c_zip CHAR(9),
                    c_phone CHAR(16),
                    c_since TIMESTAMP,
                    c_credit CHAR(2),
                    c_credit_lim DECIMAL(12,2),
                    c_discount DECIMAL(4,4),
                    c_balance DECIMAL(12,2),
                    c_ytd_payment DECIMAL(12,2),
                    c_payment_cnt INT,
                    c_delivery_cnt INT,
                    c_data VARCHAR(500),
                    PRIMARY KEY (c_w_id, c_d_id, c_id),
                    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id)
                );
                
                CREATE TABLE transaction_parameters (
                    txn_id INT PRIMARY KEY,
                    w_id INT,
                    d_id INT,
                    c_id INT,
                    c_w_id INT,
                    c_d_id INT,
                    c_last VARCHAR(20), -- TODO check
                    h_amount DECIMAL(5,2),
                    h_date TIMESTAMP,
                    datetime_ TIMESTAMP
                );
                
                -- incremental fails with this query present
                CREATE VIEW cust_enum AS
                SELECT c.c_first, c.c_middle, c.c_id,
                    c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip,
                    c.c_phone, c.c_credit, c.c_credit_lim,
                    c.c_discount, c.c_balance, c.c_since
                FROM customer AS c,
                     transaction_parameters AS t
                WHERE c.c_last = t.c_last
                  AND c.c_d_id = t.c_d_id
                  AND c.c_w_id = t.c_w_id
                ORDER BY c_first;
                
                CREATE VIEW cust_agg AS
                SELECT ARRAY_AGG(c_id ORDER BY c_first) AS cust_array
                FROM (SELECT c.c_id, c.c_first
                      FROM customer AS c,
                          transaction_parameters AS t
                      WHERE c.c_last = t.c_last
                        AND c.c_d_id = t.c_d_id
                        AND c.c_w_id = t.c_w_id
                      ORDER BY c_first);
                
                CREATE VIEW cust_med AS
                SELECT c.c_first, c.c_middle, c.c_id,
                    c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip,
                    c.c_phone, c.c_credit, c.c_credit_lim,
                    c.c_discount, c.c_balance, c.c_since
                FROM customer as c,
                     cust_agg as a,
                     transaction_parameters as t
                WHERE c.c_id = a.cust_array[(ARRAY_LENGTH(a.cust_array) / 2) + 1];
                """;
        this.compileRustTestCase(sql);
    }
}
