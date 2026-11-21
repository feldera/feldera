package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

import java.util.Locale;

/** Export ambiguous unquoted names for the default Gen-2 binder profile. */
public final class TableNameConflicts {
    public static void main(String[] args) {
        var options = new CompilerOptions();
        options.ioOptions.gen2 = true;
        var compiler = new DBSPCompiler(options);
        compiler.getSqlToRelCompiler().predefinedTableNameConflicts()
                .filter(s -> s.matches("[A-Za-z_][A-Za-z0-9_$]*"))
                .map(s -> s.toLowerCase(Locale.ROOT)).distinct().sorted()
                .forEach(System.out::println);
    }
}
