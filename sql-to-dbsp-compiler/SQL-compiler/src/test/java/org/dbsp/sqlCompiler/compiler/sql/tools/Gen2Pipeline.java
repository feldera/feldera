package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** A {@code pipeline} process of the Gen-2 engine. */
final class Gen2Pipeline {
    static final long POLL_INTERVAL_MS = 2;
    static final int LOG_TAIL_CHARS = 8000;
    static final String DEPLOYMENT_ID = "00000000-0000-0000-0000-00000000c0de";

    /** The pipeline failed, exited, or did not answer; {@code logTail} holds its last output. */
    static final class EngineException extends Exception {
        final String logTail;
        final boolean isUnreachable;

        EngineException(String message, String logTail, boolean isUnreachable) {
            super(message);
            this.logTail = logTail;
            this.isUnreachable = isUnreachable;
        }
    }

    private final Path binary;
    private final Path workDirectory;
    private final Path logPath;
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    @Nullable
    private Process process;
    private int port;
    /** Where the current program's output starts in the log. */
    private long logOffset;

    Gen2Pipeline(Path binary, Path workDirectory) {
        this.binary = binary;
        this.workDirectory = workDirectory;
        this.logPath = workDirectory.resolve("pipeline.log");
    }

    /** Runs the program of the pipeline configuration {@code config} (JSON).  Starts a new
     * process if the load is refused. */
    void load(String config) throws EngineException {
        if (this.isAlive()) {
            try {
                this.logOffset = this.logSize();
                this.request("POST", "/testing/load", config);
            } catch (EngineException refused) {
                this.start(config);
                return;
            }
            this.waitUntilRunning();
            return;
        }
        this.start(config);
    }

    private void start(String config) throws EngineException {
        this.stop();
        Path runDirectory = this.workDirectory.resolve("run");
        try {
            // A stale port file would send the requests to the previous process.
            deleteRecursively(runDirectory);
            Files.createDirectories(runDirectory);
            Path configPath = this.workDirectory.resolve("config.json");
            Files.writeString(configPath, config, StandardCharsets.UTF_8);
            this.logOffset = this.logSize();
            this.process = new ProcessBuilder(this.binary.toString(), "--config-file", configPath.toString(),
                    "--initial", "running", "--deployment-id", DEPLOYMENT_ID, "--default-port", "0",
                    "--enable-program-load")
                    .directory(runDirectory.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(this.logPath.toFile()))
                    .start();
            this.process.getOutputStream().close();
        } catch (IOException error) {
            throw new EngineException("cannot start " + this.binary + ": " + error.getMessage(), "", false);
        }
        this.port = this.readPortFile(runDirectory.resolve("port"));
        this.waitUntilRunning();
    }

    void stop() {
        if (this.process == null)
            return;
        this.process.descendants().forEach(ProcessHandle::destroyForcibly);
        this.process.destroyForcibly();
        try {
            this.process.waitFor();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
        this.process = null;
    }

    boolean isAlive() {
        return this.process != null && this.process.isAlive();
    }

    /** Runs one transaction, which the engine runs as one circuit step, and waits until the
     * outputs hold its result.  A transaction without records never starts, so the caller
     * skips a step that has no input. */
    void runStep(Map<String, byte[]> inputs) throws EngineException {
        this.request("POST", "/start_transaction", null);
        List<String> tokens = new ArrayList<>();
        for (Map.Entry<String, byte[]> input : inputs.entrySet()) {
            JsonNode answer = this.parse(this.request(
                    "POST", "/ingress/" + input.getKey() + "?format=" + Gen2Runner.DATA_FORMAT, input.getValue()));
            tokens.add(answer.get("token").asText());
        }
        this.request("POST", "/commit_transaction", null);
        for (String token : tokens)
            this.waitUntilComplete(token);
    }

    /** Waits until the outputs reflect every record ingested before {@code token} was issued. */
    private void waitUntilComplete(String token) throws EngineException {
        // The token is URL-safe base64, so the query needs no escaping.
        String path = "/completion_status?token=" + token;
        while (this.isAlive()) {
            if (this.requestJson(path).get("status").asText().equals("complete"))
                return;
            pause();
        }
        throw new EngineException("pipeline exited during the step", this.logTail(), false);
    }

    /** Asks the pipeline to compact its state. */
    void compact() throws EngineException {
        this.request("POST", "/start_compaction", null);
    }

    /** Waits until the loaded program has run its first step and the pipeline is idle. */
    void waitUntilQuiet() throws EngineException {
        while (this.isAlive()) {
            JsonNode metrics = this.requestJson("/stats").get("global_metrics");
            String state = metrics.get("state").asText();
            if (!state.equals("Running") && !state.equals("Paused"))
                throw new EngineException("pipeline state " + state, this.logTail(), false);
            long completedSteps = metrics.get("total_completed_steps").asLong();
            if (metrics.get("transaction_status").asText().equals("NoTransaction")
                    && metrics.get("total_completed_records").asLong() == metrics.get("total_input_records").asLong()
                    && completedSteps >= 1
                    && completedSteps == metrics.get("total_initiated_steps").asLong())
                return;
            pause();
        }
        throw new EngineException("pipeline exited during the step", this.logTail(), false);
    }

    /** Gathers additional pipeline information about a failure. */
    String failureText() {
        StringBuilder text = new StringBuilder(this.logTail());
        if (this.isAlive()) {
            for (String path : List.of("/status", "/stats/errors")) {
                try {
                    text.append("\n").append(new String(
                            this.request("GET", path, null), StandardCharsets.UTF_8));
                } catch (EngineException error) {
                    text.append("\n").append(error.getMessage());
                }
            }
        }
        return text.toString();
    }

    String logTail() {
        try (RandomAccessFile log = new RandomAccessFile(this.logPath.toFile(), "r")) {
            long start = Math.max(this.logOffset, log.length() - LOG_TAIL_CHARS);
            byte[] tail = new byte[(int) (log.length() - start)];
            log.seek(start);
            log.readFully(tail);
            return new String(tail, StandardCharsets.UTF_8);
        } catch (IOException error) {
            return "";
        }
    }

    private long logSize() {
        try {
            return Files.exists(this.logPath) ? Files.size(this.logPath) : 0;
        } catch (IOException error) {
            return 0;
        }
    }

    /** Waits until the pipeline writes the port number of its HTTP server to {@code portFile},
     * and returns the number. */
    private int readPortFile(Path portFile) throws EngineException {
        while (this.isAlive()) {
            try {
                if (Files.isRegularFile(portFile)) {
                    String text = Files.readString(portFile).trim();
                    if (!text.isEmpty())
                        return Integer.parseInt(text);
                }
            } catch (IOException | NumberFormatException error) {
                // The file is still being written.
            }
            pause();
        }
        throw new EngineException("pipeline exited during startup", this.logTail(), false);
    }

    private void waitUntilRunning() throws EngineException {
        while (this.isAlive()) {
            HttpResponse<byte[]> response = this.send("GET", "/status", null);
            // 503 while the program initializes; any other failure is final.
            if (response.statusCode() != 503) {
                JsonNode status = this.parse(this.check("GET", "/status", response));
                if (status.path("runtime_status").asText().equals("Running"))
                    return;
            }
            pause();
        }
        throw new EngineException("pipeline exited during initialization", this.logTail(), false);
    }

    private JsonNode requestJson(String path) throws EngineException {
        return this.parse(this.request("GET", path, null));
    }

    private JsonNode parse(byte[] body) throws EngineException {
        try {
            return Gen2Runner.MAPPER.readTree(body);
        } catch (IOException error) {
            throw new EngineException("unreadable answer: " + error.getMessage(), this.logTail(), false);
        }
    }

    private byte[] request(String method, String path, @Nullable Object body) throws EngineException {
        return this.check(method, path, this.send(method, path, body));
    }

    private byte[] check(String method, String path, HttpResponse<byte[]> response) throws EngineException {
        if (response.statusCode() / 100 == 2)
            return response.body();
        String detail = new String(response.body(), StandardCharsets.UTF_8);
        throw new EngineException(method + " " + path + " returned " + response.statusCode() + ": " + detail,
                this.logTail(), false);
    }

    /** {@code body} is a String (a JSON document) or bytes (a data stream). */
    private HttpResponse<byte[]> send(String method, String path, @Nullable Object body) throws EngineException {
        HttpRequest.BodyPublisher publisher = body == null ? HttpRequest.BodyPublishers.noBody()
                : body instanceof String text ? HttpRequest.BodyPublishers.ofString(text)
                : HttpRequest.BodyPublishers.ofByteArray((byte[]) body);
        try {
            URI uri = new URI("http", null, "127.0.0.1", this.port, path.split("\\?")[0],
                    path.contains("?") ? path.substring(path.indexOf('?') + 1) : null, null);
            HttpRequest request = HttpRequest.newBuilder(uri).method(method, publisher).build();
            return this.http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("Invalid pipeline path " + path, error);
        } catch (IOException error) {
            throw new EngineException(method + " " + path + " failed: " + error, this.logTail(), true);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new EngineException(method + " " + path + " was interrupted", this.logTail(), true);
        }
    }

    private static void pause() {
        try {
            Thread.sleep(POLL_INTERVAL_MS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path))
            return;
        try (Stream<Path> paths = Files.walk(path)) {
            for (Path entry : paths.sorted(Comparator.reverseOrder()).toList())
                Files.delete(entry);
        }
    }
}
