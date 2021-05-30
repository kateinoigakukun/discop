package discop.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Serialization {
    public static void serializeMessage(OutputStream os, Message message) throws IOException {
        os.write(message.type.length());
        os.write(message.type.getBytes());
        os.write(message.payload.length);
        os.write(message.payload);
    }

    public static Message deserializeMessage(InputStream source) throws IOException {
        var typeLength = source.read();
        if (typeLength < 0) return null;
        var typeChars = source.readNBytes(typeLength);
        var type = new String(typeChars);
        var payloadLength = source.read();
        if (payloadLength < 0) return null;
        var payload = source.readNBytes(payloadLength);
        return new Message(type, payload);
    }
}
