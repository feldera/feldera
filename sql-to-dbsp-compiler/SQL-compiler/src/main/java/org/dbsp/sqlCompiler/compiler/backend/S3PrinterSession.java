package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.CompilerMain;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Keep one request's JVM alive while the supervised Rust worker reads its S3.
 * The printer then starts a fresh compiler instance and reads the worker's output. */
public final class S3PrinterSession {
    private S3PrinterSession() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 4)
            throw new IllegalArgumentException("Expected checkpoint path, argument count, front-end and printer arguments");
        int tail = Math.addExact(2, Integer.parseInt(args[1]));
        if (tail <= 2 || tail >= args.length)
            throw new IllegalArgumentException("Expected front-end and printer arguments");
        int code = CompilerMain.runAndReportErrors(Arrays.copyOfRange(args, 2, tail)).exitCode;
        if (code == 0) {
            Files.createFile(Path.of(args[0]));
            if (System.in.read() != 1)
                throw new IllegalStateException("Rust worker did not publish its S3");
            code = CompilerMain.runAndReportErrors(Arrays.copyOfRange(args, tail, args.length)).exitCode;
        }
        System.exit(code);
    }
}
