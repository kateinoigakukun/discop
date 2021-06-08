package discop.worker;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.ByteString;
import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.stream.Collectors;

public class HttpApiServer implements Runnable {
    private HttpServer server;
    private final ClusterJobQueue jobQueue;
    private final JobCompletionPublisher publisher;
    private final ProgramStorage programStorage = new ProgramStorage();
    private final Logger logger = LoggerFactory.getLogger(HttpApiServer.class);
    private final Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    private final int port;

    static class ProgramStorage {
        private long nextProgramId = 0;
        private final HashMap<Long, ByteString> programById = new HashMap<>();

        synchronized long add(byte[] program) {
            nextProgramId += 1;
            programById.put(nextProgramId, ByteString.copyFrom(program));
            return nextProgramId;
        }

        ByteString get(long id) {
            return programById.get(id);
        }
    }

    HttpApiServer(int port, ClusterJobQueue jobQueue, JobCompletionPublisher publisher) {
        this.port = port;
        this.jobQueue = jobQueue;
        this.publisher = publisher;
    }

    @Override
    public void run() {
        try {
            var addr = new InetSocketAddress(port);
            logger.info("Listening {} for API server", addr);
            var loggerFilter = new Filter() {
                @Override
                public void doFilter(HttpExchange http, Chain chain) throws IOException {
                    try {
                        chain.doFilter(http);
                    } finally {
                        logger.info("{} {} {}",
                                http.getRequestMethod(),
                                http.getRequestURI().getPath(),
                                http.getRemoteAddress());
                    }
                }

                @Override
                public String description() {
                    return "logging http request";
                }
            };

            server = HttpServer.create(addr, 0);
            server.createContext("/upload_program", this::handleUploadProgram)
                    .getFilters().add(loggerFilter);
            server.createContext("/run_job", this::handleRunJob)
                    .getFilters().add(loggerFilter);
            server.start();
        } catch (IOException e) {
            logger.error("IO failure while server bootstrap: {}", e.toString());
        }
    }

    private void handleUploadProgram(HttpExchange exchange) throws IOException {
        byte[] wasmBytes = exchange.getRequestBody().readAllBytes();
        var id = programStorage.add(wasmBytes);

        var headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, 0);

        var response = new HttpEndpoint.UploadProgramResponse(id);
        var json = gson.toJson(response);
        var os = exchange.getResponseBody();
        os.write(json.getBytes());
        os.close();
    }

    private void handleRunJob(HttpExchange exchange) throws IOException {
        var jsonReader = new JsonReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
        HttpEndpoint.RunJobRequest request = gson.fromJson(jsonReader, HttpEndpoint.RunJobRequest.class);
        var inputs = request.inputs.stream().map(jobInput -> {
            return SchedulerMessage.JobInput.newBuilder().addAllArguments(jobInput.arguments).build();
        }).collect(Collectors.toList());

        ByteString wasmBytes = programStorage.get(request.programId);
        if (wasmBytes == null) {
            logger.error("No program found for id {}", request.programId);
            exchange.sendResponseHeaders(500, 0);
            return;
        }
        jobQueue.addJob(wasmBytes, inputs, jobId -> {
            publisher.subscribe(jobId, completion -> {
                try {
                    if (completion == null) {
                        exchange.sendResponseHeaders(500, 0);
                        return null;
                    }
                    var headers = exchange.getResponseHeaders();
                    headers.set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, 0);
                    var json = buildRunJobResponse(completion);
                    var os = exchange.getResponseBody();
                    os.write(json.getBytes());
                    os.close();
                } catch (IOException e) {
                    logger.error("IO failed to set response {}", e.toString());
                }
                return null;
            });
            return null;
        });
    }

    private String buildRunJobResponse(SchedulerMessage.JobCompletion completion) {
        return gson.toJson(completion);
    }

    public void shutdown() {
        server.stop(0);
    }
}
