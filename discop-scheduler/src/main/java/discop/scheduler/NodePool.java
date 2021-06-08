package discop.scheduler;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

// Must be thread-safe
class NodePool {
    private final List<NodeState> nodeList = new ArrayList<>();
    private static final double MAX_WORK_LOAD = 10;

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

    void addNode(NodeConnection connection) {
        this.nodeList.add(new NodeState(connection));
    }

    void removeNode(NodeConnection connection) {
        this.nodeList.removeIf((nodeState -> nodeState.connection == connection));
    }

    int assignJob(JobUnit unit) throws IOException {
        var assigned = 0;
        var partialJobCount = unit.getOriginal().getInputsCount();
        var inputList = unit.getOriginal().getInputsList();
        var sortedNode = nodeList.stream()
                .sorted(Comparator.comparingDouble(NodeState::getWorkLoad))
                .collect(Collectors.toList());
        for (var nodeState : sortedNode) {
            if (nodeState.getWorkLoad() >= MAX_WORK_LOAD) {
                return partialJobCount - assigned;
            }
            int acceptableJobs = Integer.min(
                    partialJobCount - assigned,
                    (int) (nodeState.getCoreCount() * MAX_WORK_LOAD - nodeState.executingJobs)
            );
            var assignedInputs = inputList.subList(assigned, assigned + acceptableJobs);
            assigned += acceptableJobs;
            nodeState.executingJobs += acceptableJobs;

            var jobBuilder = unit.getOriginal().toBuilder();
            jobBuilder.clearInputs();
            jobBuilder.addAllInputs(assignedInputs);
            nodeState.connection.runJob(jobBuilder.build());
        }
        return partialJobCount - assigned;
    }
}
