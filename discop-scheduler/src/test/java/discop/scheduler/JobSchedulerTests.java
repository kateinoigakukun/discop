package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class JobSchedulerTests {
    private final JobScheduler scheduler = new JobScheduler();

    @Test
    void addJob() throws Exception {
        final var job = SchedulerMessage.Job.newBuilder().build();
        scheduler.addJob(job, UUID.randomUUID());
        Assertions.assertNotNull(scheduler.nextJob());
    }
}
