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
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

class HealthHttpServer implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(HealthHttpServer.class);
    private final Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    private final int port;
    private final JobScheduler scheduler;
    private final NodePool nodePool;

    HealthHttpServer(JobScheduler scheduler, NodePool nodePool) {
        this.port = TransportConfiguration.SCHEDULER_HEALTH_DEFAULT_PORT;
        this.scheduler = scheduler;
        this.nodePool = nodePool;
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

            HttpServer server = HttpServer.create(addr, 0);
            server.createContext("/get_job_status", this::handleGetJobStatus)
                    .getFilters().add(loggerFilter);
            server.createContext("/index.html", this::handleIndexHtml)
                    .getFilters().add(loggerFilter);
            server.start();
        } catch (IOException e) {
            logger.error("IO failure while server bootstrap: {}", e.toString());
        }
    }

    static class JobResponse {
        final long jobId;
        final int childJobCount;
        final int executingChildJobs;

        JobResponse(JobScheduler.JobState jobState) {
            this.jobId = jobState.unit.getJobId();
            this.childJobCount = jobState.unit.getOriginal().getInputsCount();
            this.executingChildJobs = jobState.executingChildJobs;
        }
    }

    static class WorkerResponse {
        final String name;
        final int cores;

        public WorkerResponse(NodeConnection connection) {
            this.name = connection.getSpec().getName();
            this.cores = connection.getSpec().getCoreCount();
        }
    }

    static class Response {
        final List<JobResponse> jobs;
        final List<WorkerResponse> workers;

        public Response(List<JobResponse> jobs, List<WorkerResponse> workers) {
            this.jobs = jobs;
            this.workers = workers;
        }
    }

    private void handleIndexHtml(HttpExchange exchange) throws IOException {
        var headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "text/html");
        exchange.sendResponseHeaders(200, 0);
        var os = exchange.getResponseBody();
        var indexHtml = getClass().getClassLoader().getResourceAsStream("index.html");
        assert indexHtml != null;
        os.write(indexHtml.readAllBytes());
        os.close();
    }

    private void handleGetJobStatus(HttpExchange exchange) throws IOException {
        var headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, 0);
        var jobResponses = scheduler.getExecuting()
                .stream().map(JobResponse::new)
                .collect(Collectors.toList());
        var workerResponses = nodePool.getAllConnections()
                .map(WorkerResponse::new)
                .collect(Collectors.toList());
        var response = new Response(jobResponses, workerResponses);
        var json = gson.toJson(response);
        var os = exchange.getResponseBody();
        os.write(json.getBytes());
        os.close();
    }
}
