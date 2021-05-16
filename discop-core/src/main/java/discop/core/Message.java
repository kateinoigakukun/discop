package discop.core;

public class Message {
    public String type;
    public byte []payload;

    public Message(String type, byte[] payload) {
        this.type = type;
        this.payload = payload;
    }
}
