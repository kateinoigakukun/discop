package discop.worker;

import com.google.protobuf.ByteString;
import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;
import java.io.OutputStream;

public class ClusterJobQueue {
    final OutputStream schedulerOutgoing;
    ClusterJobQueue(OutputStream schedulerOutgoing) {
        this.schedulerOutgoing = schedulerOutgoing;
    }

    void addJob(byte[] wasmBytes) throws IOException {
        final var jobInput = SchedulerMessage.JobInput.newBuilder().build();
        final var job = SchedulerMessage.Job.newBuilder()
                .setWasmBytes(ByteString.copyFrom(wasmBytes))
                .addInputs(jobInput).build();
        final var message = new RPC.Message("AllocJob", job.toByteArray());
        RPC.Serialization.serializeMessage(schedulerOutgoing, message);
    }
}
