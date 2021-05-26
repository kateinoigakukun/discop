package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

// Must be thread-safe
class NodePool {
    List<NodeConnection> connectionList = new ArrayList<>();

    void addNode(NodeConnection connection) {
        this.connectionList.add(connection);
    }

    Iterator<NodeConnection> selectNodeForJob(SchedulerMessage.Job job) {
        return this.connectionList.iterator();
    }
}
