package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

// Must be thread-safe
class JobScheduler {
    private final LinkedBlockingQueue<JobUnit> queue;
    // Executing job unit by job id
    private final ConcurrentHashMap<Long, JobState> executing;
    private final Logger logger = LoggerFactory.getLogger(JobScheduler.class);

    private long _nextJobId = 0;

    static class JobState {
        final JobUnit unit;
        SchedulerMessage.JobUnitOutput[] outputs;
        int executingChildJobs;

        JobState(JobUnit unit) {
            this.unit = unit;
            var childCount = unit.getOriginal().getInputsCount();
            this.outputs = new SchedulerMessage.JobUnitOutput[childCount];
            this.executingChildJobs = childCount;
        }
    }

    static class Completion {
        final SchedulerMessage.JobCompletion message;
        final JobUnit unit;

        public Completion(SchedulerMessage.JobCompletion message, JobUnit unit) {
            this.message = message;
            this.unit = unit;
        }
    }

    JobScheduler() {
        this.queue = new LinkedBlockingQueue<>();
        this.executing = new ConcurrentHashMap<>();
    }

    Collection<JobState> getExecuting() {
        return executing.values();
    }

    synchronized Optional<Completion>
    completeChildJob(SchedulerMessage.JobUnitOutput output) {
        var jobState = executing.get(output.getJobId());
        if (jobState.outputs[output.getSegment()] != null) {
            logger.error("Tried to complete already completed job segment={}", output.getSegment());
            return Optional.empty();
        }
        jobState.outputs[output.getSegment()] = output;
        jobState.executingChildJobs -= 1;
        if (jobState.executingChildJobs == 0) {
            this.executing.remove(output.getJobId());
            var message = SchedulerMessage.JobCompletion.newBuilder()
                    .setJobId(output.getJobId())
                    .addAllOutputs(Arrays.asList(jobState.outputs))
                    .build();
            return Optional.of(new Completion(message, jobState.unit));
        }
        return Optional.empty();
    }

    JobUnit allocJob(SchedulerMessage.Job job, UUID producerId) {
        var newJob = job.toBuilder().setJobId(nextJobId()).build();
        return new JobUnit(newJob, producerId);
    }

    void addJob(JobUnit job) {
        queue.add(job);
    }

    JobUnit nextJob() throws InterruptedException {
        var job = queue.take();
        executing.put(job.getOriginal().getJobId(), new JobState(job));
        return job;
    }

    private long nextJobId() {
        _nextJobId += 1;
        return _nextJobId;
    }
}
