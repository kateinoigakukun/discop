package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;

import java.util.Collections;
import java.util.Iterator;

// Must be thread-safe
class NodePool {
    void addNode(NodeConnection connection) {
    }

    Iterator<NodeConnection> selectNodeForJob(SchedulerMessage.Job job) {
        Iterator<NodeConnection> iter = Collections.emptyIterator();
        return iter;
    }
}
