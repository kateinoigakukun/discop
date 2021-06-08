package discop.worker;

import java.util.ArrayList;

public class HttpEndpoint {
    static class UploadProgramResponse {
        long programId;

        public UploadProgramResponse(long programId) {
            this.programId = programId;
        }
    }
    static class JobInput {
        ArrayList<String> arguments;
    }
    static class RunJobRequest {
        long programId;
        ArrayList<JobInput> inputs;
    }
}
