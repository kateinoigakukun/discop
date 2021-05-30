package discop.scheduler;

class App {
    static int DEFAULT_PORT = 8040;

    public static void main(String[] args) throws Exception {
        var scheduler = new JobScheduler();
        var nodePool = new NodePool();
        var serverThread = new Thread(new Server(scheduler, nodePool));
        serverThread.start();
        while (true) {
            var job = scheduler.nextJob();
            // TODO: Resume only after new job came
            if (job == null) continue;
            var workerNodes = nodePool.selectNodeForJob(job);

            for (var it = workerNodes; it.hasNext(); ) {
                NodeConnection node = it.next();
                node.runJob(job);
            }
        }
    }
}
