package discop.worker;


import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

public class SchedulerConnection implements Runnable, JobCompletionPublisher {
    private final Object lock = new Object();
    private boolean terminated = false;
    private final Socket socket;
    private final JobDispatcher listener;
    private final Logger logger = LoggerFactory.getLogger(App.class);

    private final ConcurrentHashMap<Long, Function<SchedulerMessage.JobCompletion, Void>> subscribers;
    private final ConcurrentLinkedQueue<Function<RPC.Message, Void>> onResponseHandlers;

    SchedulerConnection(Socket socket, JobDispatcher listener) {
        this.socket = socket;
        this.listener = listener;
        this.subscribers = new ConcurrentHashMap<>();
        this.onResponseHandlers = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void run() {
        try {
            var input = socket.getInputStream();
            while (!socket.isInputShutdown()) {
                var incoming = RPC.Serialization.deserializeMessage(input);
                if (incoming == null) {
                    logger.error("Failed to deserialize incoming message");
                    break;
                }
                logger.debug("Received incoming message \"{}\"", incoming.type);
                handleMessage(incoming);
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        } finally {
            synchronized (lock) {
                terminated = true;
                lock.notifyAll();
            }
        }
    }

    void sendRequest(RPC.Message message, Function<RPC.Message, Void> onResponse) throws IOException {
        onResponseHandlers.add(onResponse);
        RPC.Serialization.serializeMessage(socket.getOutputStream(), message);
    }

    private void handleMessage(RPC.Message message) throws Exception {
        switch (message.type) {
            case Notification -> handleNotification(message, RPC.NotificationType.valueOf(message.subtype));
            case Request -> logger.error("Request handling is not implemented yet");
            case Response -> handleResponse(message, RPC.ResponseType.valueOf(message.subtype));
        }
    }

    private void handleNotification(RPC.Message message, RPC.NotificationType notificationType) throws IOException {
        switch (notificationType) {
            case RunAsyncJob -> {
                var runJob = SchedulerMessage.JobUnit.parseFrom(message.payload);
                listener.dispatch(runJob);
            }
            case CompleteJob -> {
                var completion = SchedulerMessage.JobCompletion.parseFrom(message.payload);
                var subscriber = subscribers.get(completion.getJobId());
                if (subscriber == null) {
                    logger.error("No completion subscription is registered for job id {}", completion.getJobId());
                    return;
                }
                subscriber.apply(completion);
                subscribers.remove(completion.getJobId());
            }
            default -> logger.warn("Unhandled incoming message \"{}\"", message.type);
        }
    }

    private void handleResponse(RPC.Message message, RPC.ResponseType responseType) {
        var handler = onResponseHandlers.poll();
        if (handler == null) {
            logger.error("No handler is registered before getting response: {} {}", responseType, message);
            return;
        }
        handler.apply(message);
    }

    @Override
    public void subscribe(long jobId, Function<SchedulerMessage.JobCompletion, Void> subscriber) {
        subscribers.put(jobId, subscriber);
    }

    void awaitTermination() throws InterruptedException {
        synchronized (lock) {
            while (!terminated) {
                lock.wait();
            }
        }
    }
}
