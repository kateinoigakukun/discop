package discop.worker;

import discop.core.RPC;
import discop.core.TransportConfiguration;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.net.Socket;

public class App {

    static int getApiServerPort() {
        var port = System.getenv("DISCOP_WORKER_API_PORT");
        var defaultPort = 8080;
        if (port == null) return defaultPort;
        try {
            return Integer.parseInt(port);
        } catch (Exception e) {
            return defaultPort;
        }
    }

    static void connectionHandshake(Socket socket, int cores) throws IOException {
        var initMessage = SchedulerMessage.NodeSpec.newBuilder()
                .setCoreCount(cores)
                .build();
        var message = RPC.Message.makeNotification(RPC.NotificationType.Init, initMessage.toByteArray());
        RPC.Serialization.serializeMessage(socket.getOutputStream(), message);
    }

    public static void main(String[] args) throws Exception {
        int cores = Runtime.getRuntime().availableProcessors();
        var socket = new Socket("localhost", TransportConfiguration.SCHEDULER_DEFAULT_PORT);
        connectionHandshake(socket, cores);
        var dispatcher = new JobDispatcher(socket.getOutputStream(), cores);
        var connection = new SchedulerConnection(socket, dispatcher);
        var clusterJobQueue = new ClusterJobQueue(connection);
        var server = new HttpApiServer(getApiServerPort(), clusterJobQueue, connection);
        new Thread(connection).start();
        new Thread(server).start();
        connection.awaitTermination();
        server.shutdown();
    }
}
