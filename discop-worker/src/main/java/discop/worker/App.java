package discop.worker;

import discop.core.RPC;
import discop.core.TransportConfiguration;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class App {

    static final Logger logger = LoggerFactory.getLogger(App.class);
    static final Integer apiPort = Integer.getInteger("discop-worker.api-port", 8080);
    static final String schedulerAddr = System.getProperty("discop-worker.scheduler-addr", "localhost");
    static final Integer schedulerPort = Integer.getInteger("discop-worker.scheduler-port", TransportConfiguration.SCHEDULER_DEFAULT_PORT);
    static final Integer hostCores = Integer.getInteger("discop-worker.cores", Runtime.getRuntime().availableProcessors());
    static final String hostName = System.getProperty("discop-worker.hostname", getHostName());

    private static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    static void connectionHandshake(Socket socket) throws IOException {
        var initMessage = SchedulerMessage.NodeSpec.newBuilder()
                .setCoreCount(App.hostCores)
                .setName(hostName)
                .build();
        var message = RPC.Message.makeNotification(RPC.NotificationType.Init, initMessage.toByteArray());
        RPC.Serialization.serializeMessage(socket.getOutputStream(), message);
    }

    public static void main(String[] args) throws Exception {
        var socket = new Socket(schedulerAddr, schedulerPort);
        logger.info("Connecting scheduler {}", socket.getRemoteSocketAddress());
        logger.info("Configure worker count to be {}", hostCores);
        connectionHandshake(socket);
        var dispatcher = new JobDispatcher(socket.getOutputStream(), hostCores);
        var connection = new SchedulerConnection(socket, dispatcher);
        var clusterJobQueue = new ClusterJobQueue(connection);
        var server = new HttpApiServer(apiPort, clusterJobQueue, connection);
        new Thread(connection).start();
        new Thread(server).start();
        connection.awaitTermination();
        server.shutdown();
    }
}
