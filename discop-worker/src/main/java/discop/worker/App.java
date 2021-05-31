package discop.worker;

import discop.core.Message;
import discop.core.Serialization;
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

    public static void main(String[] args) throws Exception {
        var socket = new Socket("localhost", TransportConfiguration.SCHEDULER_DEFAULT_PORT);
        var dispatcher = new JobDispatcher(socket.getOutputStream());
        var connection = new SchedulerIncomingConnection(socket, dispatcher);
        var clusterJobQueue = new ClusterJobQueue(socket.getOutputStream());
        var server = new HttpApiServer(clusterJobQueue, getApiServerPort());
        new Thread(connection).start();
        new Thread(server).start();
        connection.awaitTermination();
        server.shutdown();
    }
}
