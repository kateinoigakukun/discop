package discop.worker;

import discop.protobuf.msg.SchedulerMessage;
import io.github.kawamuray.wasmtime.*;
import io.github.kawamuray.wasmtime.Module;

import java.util.Arrays;
import java.util.Collection;

public class Worker {
    public void runJob(SchedulerMessage.Job message) {
        try (Store store = new Store();
             Engine engine = store.engine();
             Module module = Module.fromBinary(engine, message.getWasmBytes().toByteArray());
             Func helloFunc = WasmFunctions.wrap(store, () -> {
                 System.err.println(">>> Calling back...");
                 System.err.println(">>> Hello World!");
             })) {
            Collection<Extern> imports = Arrays.asList(Extern.fromFunc(helloFunc));
            try (Instance instance = new Instance(store, module, imports)) {
                try (Func f = instance.getFunc("run").get()) {
                    WasmFunctions.Consumer0 fn = WasmFunctions.consumer(f);
                    fn.accept();
                }
            }
        }
    }
}
