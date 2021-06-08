package discop.scheduler;

import discop.core.RPC;
import discop.protobuf.msg.SchedulerMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class NodeConnectionTests {
    private final JobScheduler scheduler = new JobScheduler();
    private final SchedulerMessage.NodeSpec nodeSpec = SchedulerMessage.NodeSpec.newBuilder().build();

    static class NopListener implements NodeConnectionListener {
        @Override
        public void onJobCompleted(NodeConnection connection, SchedulerMessage.BulkJobUnitCompletion completion) {
        }

        @Override
        public void onClosed(NodeConnection connection) {}
    }

    @Test
    void runJob() throws IOException {
        final var input = new ByteArrayInputStream(new byte[]{});
        final var output = new ByteArrayOutputStream();
        final var connection = new NodeConnection(input, output, nodeSpec, scheduler, new NopListener());
        final var job = SchedulerMessage.Job.newBuilder().build();
        connection.runJob(job);
        final var bytes = output.toByteArray();
        Assertions.assertNotEquals(bytes.length, 0);

        final var outgoing = RPC.Serialization.deserializeMessage(new ByteArrayInputStream(output.toByteArray()));
        Assertions.assertNotNull(outgoing);
        Assertions.assertEquals(outgoing.subtype, "RunAsyncJob");
    }

    @Test
    void handleAllocJob() throws Exception {
        final var job = SchedulerMessage.Job.newBuilder().build();
        final var message = RPC.Message.makeRequest(RPC.RequestType.AllocJob, job.toByteArray());
        final var messageOutput = new ByteArrayOutputStream();
        RPC.Serialization.serializeMessage(messageOutput, message);

        final var eocMessage = RPC.Message.makeNotification(RPC.NotificationType.EndOfConnection, new byte[]{});
        RPC.Serialization.serializeMessage(messageOutput, eocMessage);

        final var scheduler = new JobScheduler();
        final var input = new ByteArrayInputStream(messageOutput.toByteArray());
        final var output = new ByteArrayOutputStream();
        final var connection = new NodeConnection(input, output, nodeSpec, scheduler, new NopListener());
        connection.start();
        Assertions.assertNotNull(scheduler.nextJob());
    }
}
