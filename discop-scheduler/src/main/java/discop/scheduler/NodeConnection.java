package discop.scheduler;

import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.function.Function;

class NodeConnection implements Runnable {
    private final InputStream socketInput;
    private final OutputStream socketOutput;
    private final JobScheduler scheduler;
    private final NodeConnectionListener listener;
    private final UUID nodeId;
    private final SchedulerMessage.NodeSpec spec;
    private final Logger logger = LoggerFactory.getLogger(NodeConnection.class);

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
            logger.error("Unexpected exception happened in NodeConnection: {}", e.getMessage());
            e.printStackTrace();
            try {
                socketInput.close();
                socketOutput.close();
            } catch (IOException closingError) {
                logger.error("Unexpected exception happened in NodeConnection while closing: {}", closingError.getMessage());
                closingError.printStackTrace();
            }
        } finally {
            logger.info("Close connection to {}", nodeId);
            listener.onClosed(this);
        }
    }

    void start() throws IOException {
        while (true) {
            var message = RPC.Serialization.deserializeMessage(socketInput);
            if (message == null) {
                break;
            }
            logger.info("Receive message {} from {}", message, nodeId);
            var shouldContinue = handleMessage(message);
            if (!shouldContinue) {
                break;
            }
        }
    }

    private boolean handleMessage(RPC.Message message) throws IOException {
        switch (message.type) {
            case Notification -> {
                return handleNotification(message, RPC.NotificationType.valueOf(message.subtype));
            }
            case Request -> handleRequest(message, RPC.RequestType.valueOf(message.subtype), replyMessage -> {
                sendMessage(replyMessage);
                return null;
            });
            case Response -> {
                logger.error("Response handling is not implemented yet");
            }
        }
        return true;
    }

    private boolean handleNotification(RPC.Message message, RPC.NotificationType notificationType) throws IOException {
        switch (notificationType) {
            case NotifyJobCompleted: {
                var payload = SchedulerMessage.BulkJobUnitCompletion.parseFrom(message.payload);
                listener.onJobCompleted(this, payload);
                break;
            }
            case EndOfConnection:
                return false;
            default: {
                logger.warn("Unhandled notification: {}", notificationType);
            }
        }
        return true;
    }

    private void handleRequest(RPC.Message message, RPC.RequestType requestType,
                               Function<RPC.Message, Void> reply) throws IOException {
        switch (requestType) {
            case AllocJob: {
                var payload = SchedulerMessage.Job.parseFrom(message.payload);
                var allocated = scheduler.allocJob(payload);
                var replyMessage = RPC.Message.makeResponse(RPC.ResponseType.JobAllocated, allocated.toByteArray());
                logger.debug("Allocate a new job in scheduler id={}", allocated.getJobId());
                reply.apply(replyMessage);
                scheduler.addJob(allocated, nodeId);
                break;
            }
            default: {
            }
        }
    }

    private void sendMessage(RPC.Message message) {
        synchronized (socketOutput) {
            try {
                RPC.Serialization.serializeMessage(socketOutput, message);
                logger.info("Send message {} to {}", message, nodeId);
            } catch (IOException e) {
                logger.error("Failed to send message to node {}: {}", nodeId, e.toString());
            }
        }
    }

    void runJob(SchedulerMessage.Job job) throws IOException {
        var message = RPC.Message.makeNotification(RPC.NotificationType.RunAsyncJob, job.toByteArray());
        sendMessage(message);
    }

    void completeJob(SchedulerMessage.JobCompletion completion) throws IOException {
        var message = RPC.Message.makeNotification(RPC.NotificationType.CompleteJob, completion.toByteArray());
        sendMessage(message);
    }

    public SchedulerMessage.NodeSpec getSpec() {
        return spec;
    }

    public UUID getNodeId() {
        return nodeId;
    }
}
