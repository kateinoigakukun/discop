package discop.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RPC {
    private static int bytesToInt(byte[] b) {
        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    private static byte[] intToBytes(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public enum SchedulerRequestType {
    }
    public enum SchedulerResponseType {
        JobAllocated,
        RunAsyncJob,
        CompleteJob,
    }
    public enum SchedulerNotificationType {
    }
    public enum WorkerRequestType {
    }
    public enum WorkerResponseType {
    }

    public static class Message {
        public String type;
        public byte[] payload;

        public Message(String type, byte[] payload) {
            this.type = type;
            this.payload = payload;
        }
    }

    public static class Serialization {
        public static void serializeMessage(OutputStream os, Message message) throws IOException {
            var output = new ByteArrayOutputStream();
            output.write(message.type.length());
            output.write(message.type.getBytes());
            output.writeBytes(intToBytes(message.payload.length));
            output.write(message.payload);
            os.write(output.toByteArray());
        }

        public static Message deserializeMessage(InputStream source) throws IOException {
            var typeLength = source.read();
            if (typeLength < 0) return null;
            var typeChars = source.readNBytes(typeLength);
            var type = new String(typeChars);
            var payloadLength = bytesToInt(source.readNBytes(4));
            if (payloadLength < 0) return null;
            var payload = source.readNBytes(payloadLength);
            return new Message(type, payload);
        }
    }
}
