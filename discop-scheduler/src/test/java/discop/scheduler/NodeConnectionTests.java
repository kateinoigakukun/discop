package discop.scheduler;

import discop.core.Message;
import discop.core.Serialization;
import discop.protobuf.msg.SchedulerMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class NodeConnectionTests {
    private final JobScheduler scheduler = new JobScheduler();

    @Test
    void runJob() throws IOException {
        final var input = new ByteArrayInputStream(new byte[]{});
        final var output = new ByteArrayOutputStream();
        final var connection = new NodeConnection(input, output, scheduler);
        final var job = SchedulerMessage.Job.newBuilder().build();
        connection.runJob(job);
        final var bytes = output.toByteArray();
        Assertions.assertNotEquals(bytes.length, 0);

        final var outgoing = Serialization.deserializeMessage(new ByteArrayInputStream(output.toByteArray()));
        Assertions.assertEquals(outgoing.type, "RunAsyncJob");
    }

    @Test
    void handleAllocJob() throws IOException {
        final var job = SchedulerMessage.Job.newBuilder().build();
        final var message = new Message("AllocJob", job.toByteArray());
        final var messageOutput = new ByteArrayOutputStream();
        Serialization.serializeMessage(messageOutput, message);

        final var eocMessage = new Message("EndOfConnection", new byte[]{});
        Serialization.serializeMessage(messageOutput, eocMessage);

        final var scheduler = new JobScheduler();
        final var input = new ByteArrayInputStream(messageOutput.toByteArray());
        final var output = new ByteArrayOutputStream();
        final var connection = new NodeConnection(input, output, scheduler);
        connection.start();
        Assertions.assertNotNull(scheduler.nextJob());
    }
}
