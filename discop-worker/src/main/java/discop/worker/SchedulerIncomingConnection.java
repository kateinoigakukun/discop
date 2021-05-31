package discop.worker;


import discop.core.Message;
import discop.core.Serialization;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

public class SchedulerIncomingConnection implements Runnable {
    private final Object lock = new Object();
    private boolean terminated = false;
    private final Socket socket;
    private final JobDispatcher listener;
    private final Logger logger = LoggerFactory.getLogger(App.class);

    SchedulerIncomingConnection(Socket socket, JobDispatcher listener) {
        this.socket = socket;
        this.listener = listener;
    }

    @Override
    public void run() {
        try {
            var input = socket.getInputStream();
            while (!socket.isInputShutdown()) {
                var incoming = Serialization.deserializeMessage(input);
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

    void handleMessage(Message message) throws Exception {
        switch (message.type) {
            case "RunAsyncJob": {
                var runJob = SchedulerMessage.Job.parseFrom(message.payload);
                listener.dispatch(runJob);
                break;
            }
            default: {
                logger.warn("Unhandled incoming message \"{}\"", message.type);
                break;
            }
        }
    }

    void awaitTermination() throws InterruptedException {
        synchronized (lock) {
            while (!terminated) {
                lock.wait();
            }
        }
    }
}
