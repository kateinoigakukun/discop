package discop.cli;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import discop.protobuf.msg.*;

public class Cluster {
    static int DEFAULT_PORT = 8030;
    void start() throws IOException {
        var server = new ServerSocket(DEFAULT_PORT);
        while (true) {
            var m = ClusterMessage.Pong.newBuilder().build();
            var socket = server.accept();
            try {
                var source = socket.getInputStream();
                source.read();
                m.writeTo(socket.getOutputStream());
            } finally {
                socket.close();
            }
        }
    }
}
