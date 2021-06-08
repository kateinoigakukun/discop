package discop.worker;

import com.google.protobuf.ByteString;
import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

public class ClusterJobQueue {
    final SchedulerConnection connection;
    final Logger logger = LoggerFactory.getLogger(ClusterJobQueue.class);

    ClusterJobQueue(SchedulerConnection connection) {
        this.connection = connection;
    }

    void addJob(byte[] wasmBytes, Function<Long, Void> completion) throws IOException {
        final var jobInput = SchedulerMessage.JobInput.newBuilder().build();
        final var job = SchedulerMessage.Job.newBuilder()
                .setWasmBytes(ByteString.copyFrom(wasmBytes))
                .addInputs(jobInput).build();
        final var message = RPC.Message.makeRequest(RPC.RequestType.AllocJob, job.toByteArray());
        connection.sendRequest(message, response -> {
            if (RPC.ResponseType.valueOf(response.subtype) == RPC.ResponseType.JobAllocated) {
                try {
                    var allocated = SchedulerMessage.Job.parseFrom(response.payload);
                    completion.apply(allocated.getJobId());
                } catch (Exception e) {
                    logger.error("Failed to parse Job message from JobAllocated response");
                    completion.apply(null);
                }
            } else {
                logger.error("Unexpected AllocJob response type: {}", response.subtype);
                completion.apply(null);
            }
            return null;
        });
    }
}
