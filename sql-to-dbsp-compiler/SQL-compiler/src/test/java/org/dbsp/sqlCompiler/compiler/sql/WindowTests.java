package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.sql.quidem.ScottBaseTests;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WindowTests extends ScottBaseTests {
    static final String[] aggregates = {
            "LEAD(sal, 1) OVER (PARTITION BY deptno ORDER BY sal DESC) AS next_highest",
            "SUM(sal) OVER (PARTITION BY deptno) AS total_by_dept",
            "AVG(sal) OVER (PARTITION BY deptno) AS average_by_dept",
            """
                MAX(sal) OVER (
                     PARTITION BY deptno
                     ORDER BY sal
                     RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING
                ) AS max_in_range""",
            "FIRST_VALUE(sal) OVER (PARTITION BY deptno ORDER BY sal DESC " +
                    "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS high_in_dept"
    };

    static final String[] headers = {
            "next_highest",
            "total_by_dept",
            "average_by_dept",
            "max_in_range",
            "high_in_dept",
            "empno"
    };

    /*
        MySQL output
        | empno | total_by_dept | average_by_dept | next_highest | max_in_range | high_in_dept |
        | ----- | ------------- | --------------- | ------------ | -------------| ------------ |
        | 7369  | 10875.00      | 2175.000000     |              | 800.00       | 3000.00      |
        | 7499  | 9400.00       | 1566.666667     | 1500.00      | 1600.00      | 2850.00      |
        | 7521  | 9400.00       | 1566.666667     | 1250.00      | 1250.00      | 2850.00      |
        | 7566  | 10875.00      | 2175.000000     | 1100.00      | 2975.00      | 3000.00      |
        | 7654  | 9400.00       | 1566.666667     | 950.00       | 1250.00      | 2850.00      |
        | 7698  | 9400.00       | 1566.666667     | 1600.00      | 2850.00      | 2850.00      |
        | 7782  | 8750.00       | 2916.666667     | 1300.00      | 2450.00      | 5000.00      |
        | 7788  | 10875.00      | 2175.000000     | 3000.00      | 3000.00      | 3000.00      |
        | 7839  | 8750.00       | 2916.666667     | 2450.00      | 5000.00      | 5000.00      |
        | 7844  | 9400.00       | 1566.666667     | 1250.00      | 1500.00      | 2850.00      |
        | 7876  | 10875.00      | 2175.000000     | 800.00       | 1100.00      | 3000.00      |
        | 7900  | 9400.00       | 1566.666667     |              | 950.00       | 2850.00      |
        | 7902  | 10875.00      | 2175.000000     | 2975.00      | 3000.00      | 3000.00      |
        | 7934  | 8750.00       | 2916.666667     |              | 1300.00      | 5000.00      |
     */

    static final String[] empno = {
            "7369",
            "7499",
            "7521",
            "7566",
            "7654",
            "7698",
            "7782",
            "7788",
            "7839",
            "7844",
            "7876",
            "7900",
            "7902",
            "7934"
    };

    // validated on MySQL
    static final String[][] result = {{
        // next_highest
            "       ",
            "1500.00",
            "1250.00",
            "1100.00",
            "950.00 ",
            "1600.00",
            "1300.00",
            "3000.00",
            "2450.00",
            "1250.00",
            "800.00 ",
            "       ",
            "2975.00",
            "       "
    }, {
        // total_by_dept
            "10875.00",
            "9400.00 ",
            "9400.00 ",
            "10875.00",
            "9400.00 ",
            "9400.00 ",
            "8750.00 ",
            "10875.00",
            "8750.00 ",
            "9400.00 ",
            "10875.00",
            "9400.00 ",
            "10875.00",
            "8750.00 "
    }, {
        // average_by_dept
            "2175.000000",
            "1566.666667",
            "1566.666667",
            "2175.000000",
            "1566.666667",
            "1566.666667",
            "2916.666667",
            "2175.000000",
            "2916.666667",
            "1566.666667",
            "2175.000000",
            "1566.666667",
            "2175.000000",
            "2916.666667"
    }, {
        // max_in_range
            "800.00 ",
            "1600.00",
            "1250.00",
            "2975.00",
            "1250.00",
            "2850.00",
            "2450.00",
            "3000.00",
            "5000.00",
            "1500.00",
            "1100.00",
            "950.00 ",
            "3000.00",
            "1300.00"
    }, {
        // high_in_dept
            "3000.00",
            "2850.00",
            "2850.00",
            "3000.00",
            "2850.00",
            "2850.00",
            "5000.00",
            "3000.00",
            "5000.00",
            "2850.00",
            "3000.00",
            "2850.00",
            "3000.00",
            "5000.00"
    }
    };

    @Test
    public void testPermutations() {
        // Generate all 32 permutations of columns
        List<Integer> nums = List.of(0, 1, 2, 3, 4);
        List<List<Integer>> permutations = generatePermutations(nums);
        for (var list: permutations) {
            // TODO: this is currently broken
            if (list.get(0) != 0) continue;
            String test = this.generateTest(list);
            this.qs(test, TestOptimizations.Optimized);
        }
    }

    String generateTest(List<Integer> permutation) {
        StringBuilder test = new StringBuilder();
        test.append("SELECT ");
        for (int i: permutation) {
            test.append(aggregates[i]).append(",");
        }
        String line = "+---------";
        test.append("empno FROM emp;\n")
                .append(line.repeat(6))
                .append("+\n");
        for (int i: permutation) {
            test.append("| ")
                    .append(headers[i])
                    .append(" ");
        }
        test.append("|\n")
                .append(line.repeat(6))
                .append("+\n");
        int rows = empno.length;
        for (int row = 0; row < rows; row++) {
            for (int i: permutation) {
                String res = result[i][row];
                test.append("| ").append(res).append(" ");
            }
            test.append("| ")
                    .append(empno[row])
                    .append(" |\n");
        }
        test.append(line.repeat(6))
                .append("+\n");
        test.append("(").append(rows).append(" rows)\n");
        return test.toString();
    }

    static List<List<Integer>> generatePermutations(List<Integer> nums) {
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
