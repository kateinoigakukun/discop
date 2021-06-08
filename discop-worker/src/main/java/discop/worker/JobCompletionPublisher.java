package discop.worker;

import discop.protobuf.msg.SchedulerMessage;

import java.util.function.Function;

public interface JobCompletionPublisher {
    void subscribe(long jobId, Function<SchedulerMessage.JobCompletion, Void> subscriber);
}
