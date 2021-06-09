package discop.worker;

import java.util.ArrayList;

public class HttpEndpoint {
    static class UploadProgramResponse {
        Long programId;

        public UploadProgramResponse(Long programId) {
            this.programId = programId;
        }
    }
    static class JobInput {
        ArrayList<String> arguments;
    }
    static class RunJobRequest {
        Long programId;
        ArrayList<JobInput> inputs;
    }
}
