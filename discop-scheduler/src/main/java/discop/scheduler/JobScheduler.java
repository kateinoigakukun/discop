package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

// Must be thread-safe
class JobScheduler {
    private final LinkedBlockingQueue<SchedulerMessage.Job> queue;

    JobScheduler() {
        this.queue = new LinkedBlockingQueue<>();
    }

    void addJob(SchedulerMessage.Job job) {
        queue.add(job);
    }

    SchedulerMessage.Job nextJob() throws InterruptedException {
        return queue.take();
    }
}
