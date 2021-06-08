package discop.scheduler;

import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

class NodeConnection implements Runnable {
    private final InputStream socketInput;
    private final OutputStream socketOutput;
    private final JobScheduler scheduler;
    private final NodeConnectionListener listener;
    private final UUID nodeId;
    private final SchedulerMessage.NodeSpec spec;

    NodeConnection(InputStream socketInput, OutputStream socketOutput,
                   SchedulerMessage.NodeSpec spec,
                   JobScheduler scheduler,
                   NodeConnectionListener listener) {
        this.socketInput = socketInput;
        this.socketOutput = socketOutput;
        this.spec = spec;
        this.scheduler = scheduler;
        this.listener = listener;
        this.nodeId = UUID.randomUUID();
    }

    @Override
    public void run() {
        try {
            start();
        } catch (Exception e) {
            System.err.printf("Unexpected exception happened in NodeConnection: %s\n", e.getMessage());
            e.printStackTrace();
            try {
                socketInput.close();
                socketOutput.close();
            } catch (IOException closingError) {
                System.err.printf("Unexpected exception happened in NodeConnection while closing: %s\n", closingError.getMessage());
                closingError.printStackTrace();
            }
        } finally {
            listener.onClosed(this);
        }
    }

    void start() throws IOException {
        while (true) {
            var message = RPC.Serialization.deserializeMessage(socketInput);
            if (message == null) {
                break;
            }
            System.out.println(message.type);
            var shouldContinue = handleMessage(message);
            if (!shouldContinue) {
                break;
            }
        }
    }

    private boolean handleMessage(RPC.Message message) throws IOException {
        switch (message.type) {
            case "AllocJob": {
                var payload = SchedulerMessage.Job.parseFrom(message.payload);
                allocJob(payload);
                break;
            }
            case "NotifyJobCompleted": {
                var payload = SchedulerMessage.BulkJobUnitCompletion.parseFrom(message.payload);
                listener.onJobCompleted(this, payload);
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
        scheduler.addJob(payload, nodeId);
    }

    private void sendMessage(RPC.Message message) throws IOException {
        synchronized (socketOutput) {
            RPC.Serialization.serializeMessage(socketOutput, message);
        }
    }

    void runJob(SchedulerMessage.Job job) throws IOException {
        var message = new RPC.Message(RPC.MessageType.RunAsyncJob, job.toByteArray());
        sendMessage(message);
    }

    void completeJob(SchedulerMessage.JobCompletion completion) throws IOException {
        var message = new RPC.Message(RPC.MessageType.CompleteJob, completion.toByteArray());
        sendMessage(message);
    }

    public SchedulerMessage.NodeSpec getSpec() {
        return spec;
    }

    public UUID getNodeId() {
        return nodeId;
    }
}
