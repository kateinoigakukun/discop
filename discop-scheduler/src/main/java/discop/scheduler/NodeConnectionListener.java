package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

public interface NodeConnectionListener {
    void onJobCompleted(NodeConnection connection, SchedulerMessage.JobOutput output);
    void onClosed(NodeConnection connection);
}
