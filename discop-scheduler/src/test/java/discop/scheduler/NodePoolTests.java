package discop.scheduler;

import discop.protobuf.msg.SchedulerMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class NodePoolTests {
    private final NodePool nodePool = new NodePool();
    private final JobScheduler scheduler = new JobScheduler();

    private NodeConnection makeNopConnection(int coreCount) {
        var nodeSpec = SchedulerMessage.NodeSpec.newBuilder()
                .setCoreCount(coreCount).build();
        var input = new ByteArrayInputStream(new byte[]{});
        var output = new ByteArrayOutputStream();
        return new NodeConnection(
                input, output, nodeSpec, scheduler,
                new NodeConnectionTests.NopListener()
        );
    }

    static class NopAssigner implements NodePool.JobAssigner {
        @Override
        public void assignJobToNode(NodeConnection connection, SchedulerMessage.JobUnit unit) {
        }
    }

    @Test
    void assignNodeWithEmptyNode() {
        var job = SchedulerMessage.Job.newBuilder()
                .addInputs(SchedulerMessage.JobInput.newBuilder().build())
                .build();
        var unit = new JobUnit(job, UUID.randomUUID());
        Assertions.assertThrows(RuntimeException.class, () -> nodePool.assignJob(unit, new NopAssigner()));
    }

    @Test
    void assignNodeWithOneNode() throws IOException {
        var job = SchedulerMessage.Job.newBuilder()
                .addInputs(SchedulerMessage.JobInput.newBuilder().build())
                .build();
        var unit = new JobUnit(job, UUID.randomUUID());
        var connection = makeNopConnection(1);
        var assigner = new NodePool.JobAssigner() {
            int assignedJobCount = 0;

            @Override
            public void assignJobToNode(NodeConnection connection, SchedulerMessage.JobUnit unit) {
                assignedJobCount++;
            }
        };
        nodePool.addNode(connection);
        nodePool.assignJob(unit, assigner);
        Assertions.assertEquals(1, assigner.assignedJobCount);
    }

    Integer[] doAssignment(int jobCount, int[] nodes) throws IOException {
        var jobBuilder = SchedulerMessage.Job.newBuilder();
        for (var i = 0; i < jobCount; i++) {
            jobBuilder.addInputs(SchedulerMessage.JobInput.newBuilder().build());
        }
        var job = jobBuilder.build();
        var unit = new JobUnit(job, UUID.randomUUID());
        var assigner = new NodePool.JobAssigner() {
            int assignedJobCount = 0;
            final HashMap<UUID, ArrayList<SchedulerMessage.JobUnit>> jobMap = new HashMap<>();

            @Override
            public void assignJobToNode(NodeConnection connection, SchedulerMessage.JobUnit unit) {
                assignedJobCount += unit.getInputsCount();
                if (jobMap.containsKey(connection.getNodeId())) {
                    var jobs = jobMap.get(connection.getNodeId());
                    jobs.add(unit);
                } else {
                    var jobs = new ArrayList<SchedulerMessage.JobUnit>();
                    jobs.add(unit);
                    jobMap.put(connection.getNodeId(), jobs);
                }
            }
        };
        var nodeIds = new ArrayList<UUID>();
        for (var nodeCount : nodes) {
            var conn = makeNopConnection(nodeCount);
            nodePool.addNode(conn);
            nodeIds.add(conn.getNodeId());
        }
        nodePool.assignJob(unit, assigner);
        Assertions.assertEquals(jobCount, assigner.assignedJobCount);

        var jobDistribution = new ArrayList<Integer>();
        for (var nodeId : nodeIds) {
            var jobsAssignedToNode = assigner.jobMap.get(nodeId);
            var jobsCountAssignedToNode = jobsAssignedToNode.stream()
                    .map(SchedulerMessage.JobUnit::getInputsCount)
                    .reduce(0, Integer::sum);
            jobDistribution.add(jobsCountAssignedToNode);
        }

        return jobDistribution.toArray(new Integer[0]);
    }

    @Test
    void assignNodeWith_5_3() throws IOException {
        Assertions.assertArrayEquals(new Integer[]{1, 2, 2}, doAssignment(5, new int[]{1, 1, 1}));
    }

    @Test
    void assignNodeWith_10_3() throws IOException {
        Assertions.assertArrayEquals(new Integer[]{3, 3, 4}, doAssignment(10, new int[]{1, 1, 1}));
    }

    @Test
    void assignNodeWith_10_4() throws IOException {
        // TODO: Should be {3, 3, 2, 2}?
        Assertions.assertArrayEquals(new Integer[]{2, 4, 2, 2}, doAssignment(10, new int[]{1, 1, 1, 1}));
    }

    @Test
    void assignNodeWith_10_3_with_10_cores() throws IOException {
        Assertions.assertArrayEquals(new Integer[]{3, 3, 4}, doAssignment(10, new int[]{
                10, 10, 10
        }));
    }

    @Test
    void assignNodeWith_10_3_with_different_cores() throws IOException {
        // TODO: SHould be {1, 1, 8}
        Assertions.assertArrayEquals(new Integer[]{3, 3, 4}, doAssignment(10, new int[]{
                1, 1, 20
        }));
    }
}
