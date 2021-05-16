package discop.scheduler;

import java.net.InetSocketAddress;
import java.util.Iterator;

class App {
    static int DEFAULT_PORT = 8040;
    public static void main(String[] args) throws Exception {
        var scheduler = new JobScheduler();
        var nodePool = new NodePool();
        var serverThread = new Thread(new Server(scheduler, nodePool));
        serverThread.start();
        while (true) {
            var job = scheduler.nextJob();
            var workerNodes = nodePool.selectNodeForJob(job);

            for (var it = workerNodes; it.hasNext(); ) {
                NodeConnection node = it.next();
                node.runJob(job);
            }
        }
    }
}
