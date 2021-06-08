package discop.worker;

import com.google.gson.Gson;
import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HttpApiServer implements Runnable {
    private HttpServer server;
    private final ClusterJobQueue jobQueue;
    private final JobCompletionPublisher publisher;
    private final Logger logger = LoggerFactory.getLogger(HttpApiServer.class);
    private final int port;

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
            server.createContext("/run_job", this::handleRunJob)
                    .getFilters().add(loggerFilter);
            server.start();
        } catch (IOException e) {
            logger.error("IO failure while server bootstrap: {}", e.toString());
        }
    }

    private void handleRunJob(HttpExchange exchange) throws IOException {
        byte[] wasmBytes = exchange.getRequestBody().readAllBytes();
        jobQueue.addJob(wasmBytes, jobId -> {
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
        var gson = new Gson();
        return gson.toJson(completion);
    }

    public void shutdown() {
        server.stop(0);
    }
}
