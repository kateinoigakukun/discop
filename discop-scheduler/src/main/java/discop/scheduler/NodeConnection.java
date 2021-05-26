package discop.scheduler;

import discop.core.Message;
import discop.core.Serialization;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class NodeConnection implements Runnable {
    final InputStream socketInput;
    final OutputStream socketOutput;
    final JobScheduler scheduler;

    NodeConnection(InputStream socketInput, OutputStream socketOutput, JobScheduler scheduler) {
        this.socketInput = socketInput;
        this.socketOutput = socketOutput;
        this.scheduler = scheduler;
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
        try {
            while (true) {
                var message = Serialization.deserializeMessage(socketInput);
                var shouldContinue = handleMessage(message);
                if (!shouldContinue) {
                    break;
                }
            }
        } finally {
            socketInput.close();
            socketOutput.close();
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
        scheduler.addJob(payload);
    }

    private void sendMessage(Message message) throws IOException {
        synchronized (socketOutput) {
            Serialization.serializeMessage(socketOutput, message);
        }
    }

    void runJob(SchedulerMessage.Job job) throws IOException {
        var payload = SchedulerMessage.RunAsyncJob.newBuilder().build();
        var message = new Message("RunAsyncJob", payload.toByteArray());
        sendMessage(message);
    }
}
