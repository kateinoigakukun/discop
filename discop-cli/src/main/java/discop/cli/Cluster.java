package discop.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import discop.protobuf.msg.*;

public class Cluster {
    static int DEFAULT_PORT = 8030;
    void start() throws IOException {
        var server = new ServerSocket(DEFAULT_PORT);
        while (true) {
            var socket = server.accept();
            try {
                var source = socket.getInputStream();
                var methodLength = source.read();
                var methodChars = source.readNBytes(methodLength);
                var method = new String(methodChars);
                handleMethodCall(method, source, socket.getOutputStream());
            } finally {
                socket.close();
            }
        }
    }

    void handleMethodCall(String method, InputStream source, OutputStream output) throws IOException {
        switch (method) {
            case "Ping": {
                var message = receiveRequest(source, ClusterMessage.Ping.parser());
                System.out.println(message.getFoo());
                var reply = ClusterMessage.Pong.newBuilder().build();
                sendResponse(output, reply);
                break;
            }
            case "RunJob": {
                var message = receiveRequest(source, ClusterMessage.RunJob.parser());
                break;
            }
            default:
                System.err.printf("Unhandled method: %s\n", method);
                break;
        }
    }

    void sendResponse(OutputStream os, com.google.protobuf.Message message) throws IOException {
        var messageBytes = message.toByteArray();
        os.write(messageBytes.length);
        os.write(messageBytes);
        os.flush();
    }

    <M extends com.google.protobuf.Message> M receiveRequest(
            InputStream is,
            com.google.protobuf.Parser<M> parser
    ) throws IOException {
        var messageLength = is.read();
        var messageBytes = is.readNBytes(messageLength);
        return parser.parseFrom(messageBytes);
    }
}
