package discop.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobExecutor {
    private final JobScheduler scheduler;
    private final NodePool nodePool;
    private final Logger logger = LoggerFactory.getLogger(JobExecutor.class);

    public JobExecutor(JobScheduler scheduler, NodePool nodePool) {
        this.scheduler = scheduler;
        this.nodePool = nodePool;
    }

    void start() throws Exception {
        while (true) {
            var job = scheduler.nextJob();
            if (job == null) {
                logger.error("JobScheduler returns unexpected null");
                return;
            }
            nodePool.assignJob(job, NodeConnection::runJob);
        }
    }
}
