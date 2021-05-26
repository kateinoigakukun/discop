package discop.scheduler;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;

class Server implements Runnable {
    static int DEFAULT_PORT = 8040;
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
        var server = new ServerSocket(DEFAULT_PORT);

        while (true) {
            var socket = server.accept();
            var connection = new NodeConnection(socket.getInputStream(), socket.getOutputStream(), scheduler);
            nodePool.addNode(connection);
            new Thread(connection).start();
        }
    }
}
