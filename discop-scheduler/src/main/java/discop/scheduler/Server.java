package discop.scheduler;

import discop.core.Serialization;
import discop.core.TransportConfiguration;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.ServerSocket;

class Server implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(Server.class);
    JobScheduler scheduler;
    NodePool nodePool;

    Server(JobScheduler scheduler, NodePool nodePool) {
        this.scheduler = scheduler;
        this.nodePool = nodePool;
    }

    @Override
    public void run() {
        try {
            start();
        } catch (IOException e) {
            System.err.printf("Unexpected exception happened in Server: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

    void start() throws IOException {
        var server = new ServerSocket(TransportConfiguration.SCHEDULER_DEFAULT_PORT);

        while (!server.isClosed()) {
            var socket = server.accept();
            var input = socket.getInputStream();
            var message = Serialization.deserializeMessage(input);
            if (!message.type.equals("Init")) {
                logger.error("Expected to receive Init message, but got {}", message.type);
                continue;
            }
            var nodeSpec = SchedulerMessage.NodeSpec.parseFrom(message.payload);
            var connection = new NodeConnection(
                socket.getInputStream(), socket.getOutputStream(),
                nodeSpec,
                scheduler,
                new NodeConnectionListener() {
                    @Override
                    public void onJobCompleted(
                        NodeConnection connection,
                        SchedulerMessage.JobOutput output) {
                        
                    }
                    @Override
                    public void onClosed(NodeConnection connection) {
                        nodePool.removeNode(connection);
                    }
            });
            nodePool.addNode(connection);
            new Thread(connection).start();
        }
    }
}
