package discop.worker;

import discop.protobuf.msg.SchedulerMessage;
import io.github.kawamuray.wasmtime.*;
import io.github.kawamuray.wasmtime.Module;
import io.github.kawamuray.wasmtime.wasi.Wasi;
import io.github.kawamuray.wasmtime.wasi.WasiConfig;

import java.util.ArrayList;

public class Worker {
    static final String MAIN_MODULE_NAME = "discop_main";

    public ArrayList<SchedulerMessage.JobOutput> runJob(SchedulerMessage.Job message) {
        var outputs = new ArrayList<SchedulerMessage.JobOutput>();
        for (var input : message.getInputsList()) {
            var output = runSingleJob(message, input);
            outputs.add(output);
        }
        return outputs;
    }

    private SchedulerMessage.JobOutput runSingleJob(SchedulerMessage.Job job, SchedulerMessage.JobInput input) {
        var args = input.getArgumentsList().toArray(new String[0]);
        var wasiConfig = new WasiConfig(args, new WasiConfig.PreopenDir[0]);
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

            var results = entrypoint.func().call();
            if (results.length != 1) {
                throw new RuntimeException(
                        String.format(
                                "Expected \"_start\" returns 1 value but actually %d values", results.length));
            }
            var exitCode = results[0];
            if (exitCode.getType() != Val.Type.I32) {
                throw new RuntimeException(
                        String.format(
                                "Expected \"_start\" returns i32 but actually %s", exitCode));
            }

            var output = SchedulerMessage.JobOutput.newBuilder()
                    .setStdout("__TODO__")
                    .setExitCode(exitCode.i32())
                    .build();
            return output;
        }
    }
}
