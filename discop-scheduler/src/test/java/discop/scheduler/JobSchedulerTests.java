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
        final var unit = scheduler.allocJob(job, UUID.randomUUID());
        scheduler.addJob(unit);
        Assertions.assertNotNull(scheduler.nextJob());
    }
}
