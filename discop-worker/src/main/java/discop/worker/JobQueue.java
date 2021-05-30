package discop.worker;

import com.google.protobuf.ByteString;
import discop.core.Message;
import discop.core.Serialization;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.io.OutputStream;

public class JobQueue {
    final OutputStream schedulerOutgoing;
    JobQueue(OutputStream schedulerOutgoing) {
        this.schedulerOutgoing = schedulerOutgoing;
    }

    void addJob(byte[] wasmBytes) throws IOException {
        final var jobInput = SchedulerMessage.JobInput.newBuilder().build();
        final var job = SchedulerMessage.Job.newBuilder()
                .setWasmBytes(ByteString.copyFrom(wasmBytes))
                .addInputs(jobInput).build();
        final var message = new Message("AllocJob", job.toByteArray());
        Serialization.serializeMessage(schedulerOutgoing, message);
    }
}
