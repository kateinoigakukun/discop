package discop.scheduler;

// import discop.protobuf.msg.JobOutput;

public interface NodeConnectionListener {
    // void onJobCompleted(NodeConnection connection, JobOutput output);
    void onClosed(NodeConnection connection);
}
