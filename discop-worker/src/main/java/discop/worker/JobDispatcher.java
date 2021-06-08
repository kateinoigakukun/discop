package discop.worker;

import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.io.OutputStream;

public class JobDispatcher {
    private final OutputStream schedulerOutgoing;
    JobDispatcher(OutputStream schedulerOutgoing) {
        this.schedulerOutgoing = schedulerOutgoing;
    }
    void dispatch(SchedulerMessage.JobUnit job) {
        new Thread() {
            @Override
            public void run() {
                try {
                    var worker = new Worker();
                    var output = worker.runJob(job);
                    var completion = SchedulerMessage.BulkJobUnitCompletion
                            .newBuilder()
                            .addAllOutputs(output).build();
                    var message = new RPC.Message("NotifyJobCompleted", completion.toByteArray());
                    RPC.Serialization.serializeMessage(schedulerOutgoing, message);
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
        }.start();
    }
}
