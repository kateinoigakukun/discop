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

    public enum MessageType {
        // Notification doesn't expect reply
        Notification,
        // Request and Response should be paired and they should be sequential
        Request,
        Response,
    }
    public enum NotificationType {
        // Worker Notification
        Init,
        NotifyJobCompleted,
        EndOfConnection,
        // Scheduler Notification
        CompleteJob,
        RunAsyncJob,
    }

    public enum RequestType {
        // Worker Request
        AllocJob,
    }

    public enum ResponseType {
        // Scheduler Response
        JobAllocated,
    }

    public static class Message {
        public MessageType type;
        public String subtype;
        public byte[] payload;

        private Message(MessageType type, String subtype, byte[] payload) {
            this.type = type;
            this.subtype = subtype;
            this.payload = payload;
        }
        public static Message makeResponse(ResponseType type, byte[] payload) {
            return new Message(MessageType.Response, type.toString(), payload);
        }
        public static Message makeRequest(RequestType type, byte[] payload) {
            return new Message(MessageType.Request, type.toString(), payload);
        }
        public static Message makeNotification(NotificationType type, byte[] payload) {
            return new Message(MessageType.Notification, type.toString(), payload);
        }
    }

    public static class Serialization {
        private static void serializeString(OutputStream os, String str) throws IOException {
            os.write(str.length());
            os.write(str.getBytes());
        }
        public static void serializeMessage(OutputStream os, Message message) throws IOException {
            var output = new ByteArrayOutputStream();
            serializeString(output, message.type.toString());
            serializeString(output, message.subtype);
            output.writeBytes(intToBytes(message.payload.length));
            output.write(message.payload);
            os.write(output.toByteArray());
        }

        private static String deserializeString(InputStream source) throws IOException {
            var length = source.read();
            if (length < 0) return null;
            var chars = source.readNBytes(length);
            return new String(chars);
        }
        public static Message deserializeMessage(InputStream source) throws IOException {
            var typeString = deserializeString(source);
            if (typeString == null) return null;
            var type = MessageType.valueOf(typeString);

            var subtypeString = deserializeString(source);
            if (subtypeString == null) return null;

            var payloadLength = bytesToInt(source.readNBytes(4));
            if (payloadLength < 0) return null;
            var payload = source.readNBytes(payloadLength);
            return new Message(type, subtypeString, payload);
        }
    }
}
