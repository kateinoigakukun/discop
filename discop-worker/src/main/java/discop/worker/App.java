package discop.worker;

import discop.core.RPC;
import discop.core.TransportConfiguration;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.net.Socket;

public class App {

    static final Integer apiPort = Integer.parseInt(System.getProperty("discop-worker.api-port", "8080"));
    static final String schedulerAddr = System.getProperty("discop-worker.scheduler-addr", "localhost");
    static final Integer schedulerPort = Integer.parseInt(
            System.getProperty("discop-worker.scheduler-port", "8040")
    );

    static void connectionHandshake(Socket socket, int cores) throws IOException {
        var initMessage = SchedulerMessage.NodeSpec.newBuilder()
                .setCoreCount(cores)
                .build();
        var message = RPC.Message.makeNotification(RPC.NotificationType.Init, initMessage.toByteArray());
        RPC.Serialization.serializeMessage(socket.getOutputStream(), message);
    }

    public static void main(String[] args) throws Exception {
        int cores = Runtime.getRuntime().availableProcessors();
        var socket = new Socket(schedulerAddr, schedulerPort);
        connectionHandshake(socket, cores);
        var dispatcher = new JobDispatcher(socket.getOutputStream(), cores);
        var connection = new SchedulerConnection(socket, dispatcher);
        var clusterJobQueue = new ClusterJobQueue(connection);
        var server = new HttpApiServer(apiPort, clusterJobQueue, connection);
        new Thread(connection).start();
        new Thread(server).start();
        connection.awaitTermination();
        server.shutdown();
    }
}
