package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

import java.io.IOException;

public interface NodeConnectionListener {
    void onJobCompleted(NodeConnection connection, SchedulerMessage.BulkJobUnitCompletion completion) throws IOException;
    void onClosed(NodeConnection connection);
}
