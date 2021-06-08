package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

// Must be thread-safe
class JobScheduler {
    private final LinkedBlockingQueue<JobUnit> queue;
    private final ConcurrentHashMap<UUID, JobUnit> executing;

    JobScheduler() {
        this.queue = new LinkedBlockingQueue<>();
        this.executing = new ConcurrentHashMap<>();
    }

    void addJob(SchedulerMessage.Job job, UUID producerId) {
        queue.add(new JobUnit(job, producerId));
    }

    JobUnit nextJob() throws InterruptedException {
        return queue.take();
    }
}
