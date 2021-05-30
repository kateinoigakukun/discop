package discop.worker;

import discop.core.Message;
import discop.core.Serialization;
import discop.core.TransportConfiguration;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

public class App {
    static class SchedulerConnection implements Runnable {
        private final Object lock = new Object();
        private boolean terminated = false;
        private final Socket socket;
        private final Logger logger = LoggerFactory.getLogger(App.class);

        SchedulerConnection(Socket socket) {
            this.socket = socket;
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
                    var worker = new Worker();
                    worker.experimentalRunJob(runJob);
                }
                default: {
                    logger.warn("Unhandled incoming message \"{}\"", message.type);
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

    public static void main(String[] args) throws Exception {
        var socket = new Socket("localhost", TransportConfiguration.SCHEDULER_DEFAULT_PORT);
        var connection = new SchedulerConnection(socket);
        var server = new HttpApiServer(socket.getOutputStream());
        new Thread(connection).start();
        new Thread(server).start();
        connection.awaitTermination();
        server.shutdown();
    }
}
