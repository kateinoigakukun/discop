package discop.scheduler;

class App {
    static int DEFAULT_PORT = 8040;

    public static void main(String[] args) throws Exception {
        var scheduler = new JobScheduler();
        var nodePool = new NodePool();
        var serverThread = new Thread(new Server(scheduler, nodePool));
        serverThread.start();
        var executor = new JobExecutor(scheduler, nodePool);
        executor.start();
    }
}
