package discop.cli;


import com.google.protobuf.ByteString;
import discop.protobuf.msg.ClusterMessage;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;

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

    void queueJob() throws Exception {
        byte[] wasmBytes = {
                0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00, 0x02, 0x0a,
                0x01, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07,
                0x01, 0x03, 0x72, 0x75, 0x6e, 0x00, 0x01, 0x0a, 0x06, 0x01, 0x04, 0x00, 0x10, 0x00, 0x0b
        };
        var message = ClusterMessage.RunJob.newBuilder().setWasiBytes(ByteString.copyFrom(wasmBytes)).build();
        var socket = new Socket("localhost", Cluster.DEFAULT_PORT);
        var os = new BufferedOutputStream(socket.getOutputStream());
        sendRequest(os, "RunJob", message);
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
