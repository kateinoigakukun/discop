package discop.worker;

import discop.protobuf.msg.SchedulerMessage;
import io.github.kawamuray.wasmtime.*;
import io.github.kawamuray.wasmtime.Module;
import io.github.kawamuray.wasmtime.wasi.Wasi;
import io.github.kawamuray.wasmtime.wasi.WasiConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.Callable;

public class Worker implements Callable<SchedulerMessage.JobUnitOutput> {
    static final String MAIN_MODULE_NAME = "discop_main";
    private final Logger logger = LoggerFactory.getLogger(Worker.class);
    SchedulerMessage.JobUnit job;
    SchedulerMessage.JobUnitInput input;

    public Worker(SchedulerMessage.JobUnit job, SchedulerMessage.JobUnitInput input) {
        this.job = job;
        this.input = input;
    }

    @Override
    public SchedulerMessage.JobUnitOutput call() throws Exception {
        return runSingleJob();
    }

    SchedulerMessage.JobUnitOutput runSingleJob() throws IOException {
        var args = input.getInput().getArgumentsList().toArray(new String[0]);
        var stdoutPath = Files.createTempFile("discop-worker-wasi-stdout", "");
        var wasiConfig = new WasiConfig(
                args, new WasiConfig.PreopenDir[0],
                stdoutPath.toString(), null
        );
        try (Store store = new Store();
             Linker linker = new Linker(store);
             Wasi wasi = new Wasi(store, wasiConfig);
             Engine engine = store.engine();
             Module module = Module.fromBinary(engine, job.getWasmBytes().toByteArray());
        ) {
            wasi.addToLinker(linker);
            linker.module(MAIN_MODULE_NAME, module);
            var entrypoint = linker.getOneByName(MAIN_MODULE_NAME, "_start");
            if (entrypoint.type() == null) {
                throw new RuntimeException("No \"_start\" function in wasi binary");
            }
            if (entrypoint.type() != Extern.Type.FUNC) {
                throw new RuntimeException("Expected \"_start\" to be a function");
            }
            entrypoint.func().call();
            var stdoutContent = Files.readString(stdoutPath);
            return SchedulerMessage.JobUnitOutput.newBuilder()
                    .setJobId(job.getJobId())
                    .setSegment(input.getSegment())
                    .setStdout(stdoutContent)
                    .setExitCode(0)
                    .build();
        }
    }
}
