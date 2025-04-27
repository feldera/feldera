package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.sql.quidem.ScottBaseTests;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WindowTests extends ScottBaseTests {
    record PermutationTest(
            String[] aggregates,
            String fixedColumn,
            String[] headers,
            String[] fixedColumnValues,
            String[][] result) {
        List<String> generateAll() {
            List<List<Integer>> permutations = generatePermutations(this.headers.length);
            return Linq.map(permutations, this::generateTest);
        }

        String generateTest(List<Integer> permutation) {
            Assert.assertEquals(permutation.size(), aggregates.length);
            final String line = "+---------";

            StringBuilder test = new StringBuilder();
            test.append("SELECT ");
            for (int i : permutation) {
                test.append(this.aggregates[i]).append(",\n");
            }
            test.append(this.fixedColumn)
                    .append(" FROM emp;\n")
                    .append(line.repeat(6))
                    .append("+\n");
            for (int i : permutation) {
                test.append("| ")
                        .append(headers[i])
                        .append(" ");
            }
            test.append("| ").append(this.fixedColumn).append(" ");
            test.append("|\n")
                    .append(line.repeat(6))
                    .append("+\n");
            int rows = this.fixedColumnValues.length;
            for (int row = 0; row < rows; row++) {
                for (int i : permutation) {
                    String res = result[row][i];
                    test.append("| ").append(res).append(" ");
                }
                test.append("| ")
                        .append(this.fixedColumnValues[row])
                        .append(" |\n");
            }
            test.append(line.repeat(6))
                    .append("+\n");
            test.append("(").append(rows).append(" rows)\n");
            return test.toString();
        }
    }

    record QueriesAndOutput(
            String[] aggregates,
            String fixedColumn,
            String output) {
        PermutationTest createTest() {
            String[] rows = output.split("\n");
            Assert.assertTrue(rows.length >= 2);
            final int dataRows = rows.length - 2;
            String header = rows[0];
            String[] columns = header.substring(1).split("\\|");
            String[] headers = new String[columns.length - 1];

            Assert.assertEquals(headers.length, aggregates.length);
            int index = 0;
            for (String col : columns) {
                col = col.trim();
                if (col.equals(fixedColumn))
                    continue;
                headers[index] = col;
                index++;
            }

            String[][] result = new String[dataRows][headers.length];
            String[] fixedColumnValues = new String[dataRows];
            for (int i = 2; i < rows.length; i++) {
                String row = rows[i];
                String[] values = row.substring(1).split("\\|");
                Assert.assertEquals(columns.length, values.length);
                index = 0;
                for (int j = 0; j < columns.length; j++) {
                    String value = values[j].trim();
                    if (columns[j].trim().equals(this.fixedColumn)) {
                        fixedColumnValues[i-2] = value;
                    } else {
                        result[i-2][index] = value;
                        index++;
                    }
                }
            }
            return new PermutationTest(aggregates, fixedColumn, headers, fixedColumnValues, result);
        }
    }

    static QueriesAndOutput q0 = new QueriesAndOutput(new String[]{
            "LEAD(sal, 1) OVER (PARTITION BY deptno ORDER BY sal DESC) AS next_highest",
            "SUM(sal) OVER (PARTITION BY deptno) AS total_by_dept",
            """
                MAX(sal) OVER (
                     PARTITION BY deptno
                     ORDER BY sal
                     RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING
                ) AS max_in_range""",
            "FIRST_VALUE(sal) OVER (PARTITION BY deptno ORDER BY sal DESC " +
                    "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS high_in_dept"
    }, "empno", """
            | empno | next_highest | total_by_dept | max_in_range | high_in_dept |
            | ----- | ------------ | ------------- | -------------| ------------ |
            | 7369  |              | 10875.00      | 800.00       | 3000.00      |
            | 7499  | 1500.00      | 9400.00       | 1600.00      | 2850.00      |
            | 7521  | 1250.00      | 9400.00       | 1250.00      | 2850.00      |
            | 7566  | 1100.00      | 10875.00      | 2975.00      | 3000.00      |
            | 7654  | 950.00       | 9400.00       | 1250.00      | 2850.00      |
            | 7698  | 1600.00      | 9400.00       | 2850.00      | 2850.00      |
            | 7782  | 1300.00      | 8750.00       | 2450.00      | 5000.00      |
            | 7788  | 3000.00      | 10875.00      | 3000.00      | 3000.00      |
            | 7839  | 2450.00      | 8750.00       | 5000.00      | 5000.00      |
            | 7844  | 1250.00      | 9400.00       | 1500.00      | 2850.00      |
            | 7876  | 800.00       | 10875.00      | 1100.00      | 3000.00      |
            | 7900  |              | 9400.00       | 950.00       | 2850.00      |
            | 7902  | 2975.00      | 10875.00      | 3000.00      | 3000.00      |
            | 7934  |              | 8750.00       | 1300.00      | 5000.00      |""");

    static QueriesAndOutput q1 = new QueriesAndOutput(new String[] {
            "SUM(sal) OVER (PARTITION BY deptno) AS total_by_dept",
            "LEAD(sal, 1) OVER (PARTITION BY deptno ORDER BY sal DESC) AS next_highest",
            "AVG(sal) OVER (PARTITION BY deptno) AS avg_by_dept",
            "LAG(sal, 1) OVER (PARTITION BY deptno ORDER BY sal DESC) AS prev_highest"
    }, "empno", """
            | empno | total_by_dept | next_highest | avg_by_dept | prev_highest |
            | ----- | ------------- | ------------ | ----------- | ------------ |
            | 7369  | 10875.00      |              | 2175.000000 | 1100.00      |
            | 7499  | 9400.00       | 1500.00      | 1566.666667 | 2850.00      |
            | 7521  | 9400.00       | 1250.00      | 1566.666667 | 1500.00      |
            | 7566  | 10875.00      | 1100.00      | 2175.000000 | 3000.00      |
            | 7654  | 9400.00       | 950.00       | 1566.666667 | 1250.00      |
            | 7698  | 9400.00       | 1600.00      | 1566.666667 |              |
            | 7782  | 8750.00       | 1300.00      | 2916.666667 | 5000.00      |
            | 7788  | 10875.00      | 3000.00      | 2175.000000 |              |
            | 7839  | 8750.00       | 2450.00      | 2916.666667 |              |
            | 7844  | 9400.00       | 1250.00      | 1566.666667 | 1600.00      |
            | 7876  | 10875.00      | 800.00       | 2175.000000 | 2975.00      |
            | 7900  | 9400.00       |              | 1566.666667 | 1250.00      |
            | 7902  | 10875.00      | 2975.00      | 2175.000000 | 3000.00      |
            | 7934  | 8750.00       |              | 2916.666667 | 2450.00      |""");

    @Test
    public void testQ0() {
        PermutationTest test = q0.createTest();
        List<String> permutations = test.generateAll();
        for (var t : permutations) {
            this.qs(t, TestOptimizations.Optimized);
        }
    }

    @Test
    public void testQ1() {
        PermutationTest test = q1.createTest();
        List<String> permutations = test.generateAll();
        for (var t : permutations) {
            this.qs(t, TestOptimizations.Optimized);
        }
    }

    static List<List<Integer>> generatePermutations(int size) {
        List<Integer> nums = new ArrayList<>();
        for (int i = 0; i < size; i++)
            nums.add(i);
        List<List<Integer>> result = new ArrayList<>();
        backtrack(nums, new ArrayList<>(), result);
        return result;
    }

    static void backtrack(List<Integer> nums, List<Integer> tempList, List<List<Integer>> result) {
        if (tempList.size() == nums.size()) {
            result.add(new ArrayList<>(tempList));
        } else {
            for (int i = 0; i < nums.size(); i++) {
                if (tempList.contains(nums.get(i))) continue;
                tempList.add(nums.get(i));
                backtrack(nums, tempList, result);
                tempList.remove(tempList.size() - 1);
            }
        }
    }
}
