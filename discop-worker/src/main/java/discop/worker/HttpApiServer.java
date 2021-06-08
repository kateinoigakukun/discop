package discop.worker;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
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
            server.createContext("/run_job", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    byte[] wasmBytes = exchange.getRequestBody().readAllBytes();
                    try {
                        jobQueue.addJob(wasmBytes);
//                        publisher.subscribe();
                        exchange.sendResponseHeaders(200, 0);
                    } catch (IOException e) {
                        exchange.sendResponseHeaders(500, 0);
                    }
                    exchange.getResponseBody().close();
                }
            })
                    .getFilters().add(loggerFilter);
            server.start();
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    public void shutdown() {
        server.stop(0);
    }
}
