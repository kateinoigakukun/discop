package discop.worker;

import discop.core.Message;
import discop.core.Serialization;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.io.OutputStream;

public class JobDispatcher {
    private final OutputStream schedulerOutgoing;
    JobDispatcher(OutputStream schedulerOutgoing) {
        this.schedulerOutgoing = schedulerOutgoing;
    }
    void dispatch(SchedulerMessage.Job job) {
        new Thread() {
            @Override
            public void run() {
                try {
                    var worker = new Worker();
                    var output = worker.runJob(job);
                    var completion = SchedulerMessage.NotifyJobCompleted
                            .newBuilder()
                            .setJobId(job.getJobId())
                            .addAllOutputs(output).build();
                    var message = new Message("NotifyJobCompleted", completion.toByteArray());
                    Serialization.serializeMessage(schedulerOutgoing, message);
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
        }.start();
    }
}
