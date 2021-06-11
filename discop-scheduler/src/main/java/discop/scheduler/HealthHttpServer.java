package discop.scheduler;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import discop.core.TransportConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.stream.Collectors;

class HealthHttpServer implements Runnable {
    private HttpServer server;
    private final Logger logger = LoggerFactory.getLogger(HealthHttpServer.class);
    private final Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    private final int port;
    private final JobScheduler scheduler;

    HealthHttpServer(JobScheduler scheduler) {
        this.port = TransportConfiguration.SCHEDULER_HEALTH_DEFAULT_PORT;
        this.scheduler = scheduler;
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
            server.createContext("/get_job_status", this::handleGetJobStatus)
                    .getFilters().add(loggerFilter);
            server.start();
        } catch (IOException e) {
            logger.error("IO failure while server bootstrap: {}", e.toString());
        }
    }

    static class Response {
        final long jobId;
        final int childJobCount;
        final int executingChildJobs;

        Response(JobScheduler.JobState jobState) {
            this.jobId = jobState.unit.getJobId();
            this.childJobCount = jobState.unit.getOriginal().getInputsCount();
            this.executingChildJobs = jobState.executingChildJobs;
        }
    }

    private void handleGetJobStatus(HttpExchange exchange) throws IOException {
        byte[] wasmBytes = exchange.getRequestBody().readAllBytes();
        var headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, 0);
        var response = scheduler.getExecuting()
                .stream().map((state) -> new Response(state))
                .collect(Collectors.toList());
        var json = gson.toJson(response);
        var os = exchange.getResponseBody();
        os.write(json.getBytes());
        os.close();
    }
}
