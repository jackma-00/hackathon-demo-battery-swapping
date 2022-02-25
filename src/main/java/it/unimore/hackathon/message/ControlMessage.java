package it.unimore.hackathon.message;

import java.util.Map;

public class ControlMessage extends GenericMessage{

    public ControlMessage() {
    }

    public ControlMessage(String type, Map<String, Object> metadata) {
        super(type, metadata);
    }

    public ControlMessage(String type, long timestamp, Map<String, Object> metadata) {
        super(type, timestamp, metadata);
    }
}
