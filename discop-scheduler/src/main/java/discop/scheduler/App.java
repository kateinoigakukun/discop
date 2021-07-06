package discop.scheduler;

class App {

    public static void main(String[] args) throws Exception {
        var scheduler = new JobScheduler();
        var nodePool = new NodePool();
        var serverThread = new Thread(new Server(scheduler, nodePool));
        serverThread.start();
        var healthServerThread = new Thread(new HealthHttpServer(scheduler, nodePool));
        healthServerThread.start();
        var executor = new JobExecutor(scheduler, nodePool);
        executor.start();
    }
}
