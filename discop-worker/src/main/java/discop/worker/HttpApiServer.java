package discop.worker;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import discop.core.Message;
import discop.core.Serialization;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class HttpApiServer implements Runnable {
    private HttpServer server;
    private final OutputStream outgoing;
    private final Logger logger = LoggerFactory.getLogger(HttpApiServer.class);
    private final int port;

    HttpApiServer(OutputStream outgoing, int port) {
        this.outgoing = outgoing;
        this.port = port;
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
            server.createContext("/new_job", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    byte[] wasmBytes = exchange.getRequestBody().readAllBytes();
                    final var jobInput = SchedulerMessage.JobInput.newBuilder().build();
                    final var job = SchedulerMessage.Job.newBuilder()
                            .setWasmBytes(ByteString.copyFrom(wasmBytes))
                            .addInputs(jobInput).build();
                    final var message = new Message("AllocJob", job.toByteArray());
                    Serialization.serializeMessage(outgoing, message);
                    exchange.sendResponseHeaders(200, 0);
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
