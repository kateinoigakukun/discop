package discop.worker;

import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.*;

public class JobDispatcher {
    private final OutputStream schedulerOutgoing;
    private final Logger logger = LoggerFactory.getLogger(JobDispatcher.class);
    private final ExecutorService executorService;

    JobDispatcher(OutputStream schedulerOutgoing, int nThreads) {
        this.schedulerOutgoing = schedulerOutgoing;
        this.executorService = Executors.newFixedThreadPool(nThreads);
    }

    void dispatch(SchedulerMessage.JobUnit job) {
        new Thread() {
            @Override
            public void run() {
                try {
                    var futures = new ArrayList<Future<SchedulerMessage.JobUnitOutput>>();
                    for (var input : job.getInputsList()) {
                        var worker = new Worker(job, input);
                        var future = executorService.submit(worker);
                        futures.add(future);
                    }
                    for (var future : futures) {
                        var output = future.get();
                        var completionBuilder = SchedulerMessage.BulkJobUnitCompletion
                                .newBuilder();
                        completionBuilder.addOutputs(output);
                        var completion = completionBuilder.build();
                        var message = RPC.Message.makeNotification(RPC.NotificationType.NotifyJobCompleted, completion.toByteArray());
                        RPC.Serialization.serializeMessage(schedulerOutgoing, message);
                        logger.info("Job completed: stdout={}, exitCode={}", output.getStdout(), output.getExitCode());
                    }
                } catch (Exception e) {
                    logger.error("Exception raised while collecting results of job: {}", e.toString());
                }
            }
        }.start();
    }
}
