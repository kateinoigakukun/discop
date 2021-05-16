package discop.scheduler;

import discop.core.Message;
import discop.core.Serialization;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

class NodeConnection implements Runnable {
    Socket socket;
    JobScheduler scheduler;
    Serialization serialization;

    NodeConnection(Socket socket, JobScheduler scheduler) {
        this.socket = socket;
        this.scheduler = scheduler;
        this.serialization = new Serialization();
    }

    @Override
    public void run() {
        try {
            start();
        } catch (IOException e) {
            System.err.printf("Unexpected exception happened in NodeConnection: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

    void start() throws IOException {
        var serialization = new Serialization();
        try {
            var source = socket.getInputStream();
            while (true) {
                var message = serialization.deserializeMessage(source);
                var shouldContinue = handleMessage(message);
                if (!shouldContinue) {
                    break;
                }
            }
        } finally {
            socket.close();
        }
    }

    private boolean handleMessage(Message message) throws IOException {
        switch (message.type) {
            case "AllocJob": {
                var payload = SchedulerMessage.Job.parseFrom(message.payload);
                allocJob(payload);
                break;
            }
            case "EndOfConnection": {
                return false;
            }
            default: {
                System.err.printf("Unhandled method: %s\n", message.type);
                break;
            }
        }
        return true;
    }

    private void allocJob(SchedulerMessage.Job payload) {
    }

    private void sendMessage(Message message) throws IOException {
        synchronized (socket) {
            serialization.serializeMessage(socket.getOutputStream(), message);
        }
    }

    void runJob(SchedulerMessage.Job job) throws IOException {
        var payload = SchedulerMessage.RunAsyncJob.newBuilder().build();
        var message = new Message("RunAsyncJob", payload.toByteArray());
        sendMessage(message);
    }
}
