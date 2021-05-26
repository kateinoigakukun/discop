package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

import java.util.concurrent.ConcurrentLinkedQueue;

// Must be thread-safe
class JobScheduler {
    private ConcurrentLinkedQueue<SchedulerMessage.Job> queue;

    JobScheduler() {
        this.queue = new ConcurrentLinkedQueue<>();
    }

    void addJob(SchedulerMessage.Job job) {
        queue.add(job);
    }

    SchedulerMessage.Job nextJob() {
        return queue.poll();
    }
}
