package discop.worker;


import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class SchedulerConnection implements Runnable, JobCompletionPublisher {
    private final Object lock = new Object();
    private boolean terminated = false;
    private final Socket socket;
    private final JobDispatcher listener;
    private final Logger logger = LoggerFactory.getLogger(App.class);

    private final ConcurrentHashMap<Long, Function<SchedulerMessage.JobCompletion, Void>> subscribers;

    SchedulerConnection(Socket socket, JobDispatcher listener) {
        this.socket = socket;
        this.listener = listener;
        this.subscribers = new ConcurrentHashMap<>();
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

    void sendRequest(RPC.Message message) throws IOException {
        RPC.Serialization.serializeMessage(socket.getOutputStream(), message);
    }

    void handleMessage(RPC.Message message) throws Exception {
        switch (message.type) {
            case "JobAllocated": {
                var job = SchedulerMessage.JobUnit.parseFrom(message.payload);
                break;
            }
            case "RunAsyncJob": {
                var runJob = SchedulerMessage.JobUnit.parseFrom(message.payload);
                listener.dispatch(runJob);
                break;
            }
            case "CompleteJob": {
                var completion = SchedulerMessage.JobCompletion.parseFrom(message.payload);
                subscribers.get(completion.getJobId()).apply(completion);
                subscribers.remove(completion.getJobId());
            }
            default: {
                logger.warn("Unhandled incoming message \"{}\"", message.type);
                break;
            }
        }
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
