package discop.scheduler;


import discop.protobuf.msg.SchedulerMessage;

import java.util.UUID;

public class JobUnit {
    private final SchedulerMessage.Job original;
    private final UUID producerId;

    public JobUnit(SchedulerMessage.Job original, UUID producerId) {
        this.original = original;
        this.producerId = producerId;
    }

    public UUID getProducerId() {
        return producerId;
    }

    public SchedulerMessage.Job getOriginal() {
        return original;
    }

    public long getJobId() {
        return original.getJobId();
    }
}

