package discop.cli;

import discop.protobuf.msg.ClusterMessage;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Client {
    void ping() throws Exception {
        var message = ClusterMessage.Ping.newBuilder().setFoo("hello").build();
        var socket = new Socket("localhost", Cluster.DEFAULT_PORT);
        var os = new BufferedOutputStream(socket.getOutputStream());
        sendRequest(os, "Ping", message);

        var is = socket.getInputStream();
        var reply = receiveResponse(is, ClusterMessage.Pong.parser());
        System.out.println(reply);
        socket.close();
    }

    void sendRequest(OutputStream os, String method, com.google.protobuf.Message message) throws IOException {
        os.write(method.length());
        os.write(method.getBytes());
        var messageBytes = message.toByteArray();
        os.write(messageBytes.length);
        os.write(messageBytes);
        os.flush();
    }

    <M extends com.google.protobuf.Message> M receiveResponse(
            InputStream is,
            com.google.protobuf.Parser<M> parser
    ) throws IOException {
        var messageLength = is.read();
        var messageBytes = is.readNBytes(messageLength);
        return parser.parseFrom(messageBytes);
    }
}
