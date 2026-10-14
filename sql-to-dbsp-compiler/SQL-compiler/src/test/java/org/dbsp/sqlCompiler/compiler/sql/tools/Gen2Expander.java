package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** An {@code ir-expander serve} process: it evaluates the compiler's Z-set literals into
 * actual Z-sets. */
final class Gen2Expander {
    static final ObjectMapper MAPPER = new ObjectMapper();

    /** A data stream and how many distinct rows it holds. */
    record Expanded(byte[] stream, int rows) {}

    private final Path binary;
    private final Path logPath;
    @Nullable
    private Process process;
    @Nullable
    private BufferedWriter requests;
    @Nullable
    private BufferedReader answers;

    Gen2Expander(Path binary, Path workDirectory) {
        this.binary = binary;
        this.logPath = workDirectory.resolve("ir-expander.log");
    }

    /** The literal {@code literal} (IR JSON) as a stream whose columns are named {@code columns}. */
    Expanded expand(String literal, List<String> columns) throws IOException {
        // The literal is spliced in as written: parsing it would round decimals that do not fit a double.
        JsonNode expanded = this.call("{\"expand\":{\"columns\":" + MAPPER.writeValueAsString(columns)
                + ",\"literal\":" + literal + "}}").get("expanded");
        return new Expanded(Base64.getDecoder().decode(expanded.get("stream").asText()), expanded.get("rows").asInt());
    }

    /** The rows whose weights differ between the literal {@code expected} and the
     * {@code actual} bytes: {@code +w (values)} only expected, {@code -w (values)} only actual. */
    List<String> compare(String expected, byte[] actual) throws IOException {
        JsonNode differences = this.call("{\"compare\":{\"actual\":\"" + Base64.getEncoder().encodeToString(actual)
                + "\",\"expected\":" + expected + "}}").get("differences");
        List<String> rows = new ArrayList<>();
        for (JsonNode row : differences)
            rows.add(row.asText());
        return rows;
    }

    private JsonNode call(String request) throws IOException {
        if (this.process == null || !this.process.isAlive())
            this.start();
        assert this.requests != null && this.answers != null;
        this.requests.write(request);
        this.requests.write('\n');
        this.requests.flush();
        String line = this.answers.readLine();
        if (line == null) {
            this.stop();
            throw new IOException("ir-expander exited: " + Files.readString(this.logPath).trim());
        }
        JsonNode answer = MAPPER.readTree(line);
        if (answer.has("error"))
            throw new IOException("ir-expander: " + answer.get("error").asText());
        return answer;
    }

    private void start() throws IOException {
        this.stop();
        this.process = new ProcessBuilder(this.binary.toString(), "serve")
                .redirectError(this.logPath.toFile())
                .start();
        this.requests = new BufferedWriter(new OutputStreamWriter(this.process.getOutputStream(), StandardCharsets.UTF_8));
        this.answers = new BufferedReader(new InputStreamReader(this.process.getInputStream(), StandardCharsets.UTF_8));
    }

    void stop() {
        if (this.process == null)
            return;
        this.process.destroyForcibly();
        this.process = null;
        this.requests = null;
        this.answers = null;
    }
}
