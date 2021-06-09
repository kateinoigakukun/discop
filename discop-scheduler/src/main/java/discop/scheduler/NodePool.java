package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

// Must be thread-safe
class NodePool {
    private final PriorityBlockingQueue<NodeState> nodeList;
    private final ConcurrentHashMap<UUID, NodeState> nodeById;
    private static final double MAX_WORK_LOAD = 10;
    private final Logger logger = LoggerFactory.getLogger(NodePool.class);

    static class NodeState {
        final NodeConnection connection;
        long executingJobs;

        NodeState(NodeConnection connection) {
            this.connection = connection;
            this.executingJobs = 0;
        }

        int getCoreCount() {
            return connection.getSpec().getCoreCount();
        }

        double getWorkLoad() {
            return executingJobs / (double) this.connection.getSpec().getCoreCount();
        }
    }

    NodePool() {
        this.nodeList = new PriorityBlockingQueue<>(1, Comparator.comparingDouble(NodeState::getWorkLoad));
        this.nodeById = new ConcurrentHashMap<>();
    }

    void addNode(NodeConnection connection) {
        var state = new NodeState(connection);
        this.nodeById.put(connection.getNodeId(), state);
        this.nodeList.add(state);
        logger.info("New node joined our cluster: {}", connection.getNodeId());
    }

    void removeNode(NodeConnection connection) {
        this.nodeList.removeIf((nodeState -> nodeState.connection == connection));
    }

    NodeConnection getConnection(UUID id) {
        return nodeById.get(id).connection;
    }

    synchronized void completeChildJob(NodeConnection connection) {
        var state = nodeById.get(connection.getNodeId());
        state.executingJobs -= 1;
    }

    synchronized void assignJob(JobUnit unit) throws IOException {
        var assigned = 0;
        var childJobCount = unit.getOriginal().getInputsCount();
        var inputList = unit.getOriginal().getInputsList();
        var nodeCount = nodeList.size();
        while (assigned < childJobCount) {
            var nodeState = nodeList.poll();
            if (nodeState == null) throw new RuntimeException("not enough node");
            int acceptableJobs = Integer.min(
                    childJobCount - assigned,
                    childJobCount / nodeCount
            );
            var assignedSegmentStart = assigned;
            var assignedInputs = inputList.subList(assigned, assigned + acceptableJobs);
            assigned += acceptableJobs;
            nodeState.executingJobs += acceptableJobs;

            var jobBuilder = SchedulerMessage.JobUnit.newBuilder()
                    .setJobId(unit.getJobId())
                    .setWasmBytes(unit.getOriginal().getWasmBytes());
            for (var segmentOffset = 0; segmentOffset < assignedInputs.size(); segmentOffset++) {
                var input = assignedInputs.get(segmentOffset);
                var unitInput = SchedulerMessage.JobUnitInput.newBuilder()
                        .setInput(input)
                        .setSegment(assignedSegmentStart + segmentOffset);
                jobBuilder.addInputs(unitInput);
            }
            nodeState.connection.runJob(jobBuilder.build());
            nodeList.put(nodeState);
        }
    }
}
