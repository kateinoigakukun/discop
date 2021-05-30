package discop.scheduler;

import discop.core.Serialization;
import discop.core.TransportConfiguration;

import java.io.IOException;
import java.net.ServerSocket;

class Server implements Runnable {
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
            var connection = new NodeConnection(socket.getInputStream(), socket.getOutputStream(), scheduler, new NodeConnectionListener() {
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
