package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

import java.util.concurrent.SynchronousQueue;

// Must be thread-safe
class JobScheduler {
    SynchronousQueue<SchedulerMessage.Job> queue;

    JobScheduler() {
        this.queue = new SynchronousQueue<>();
    }

    void addJob(SchedulerMessage.Job job) {
        queue.add(job);
    }

    SchedulerMessage.Job nextJob() throws InterruptedException {
        return queue.take();
    }
}
