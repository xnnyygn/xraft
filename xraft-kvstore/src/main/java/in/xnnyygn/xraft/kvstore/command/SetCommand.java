package in.xnnyygn.xraft.kvstore.command;

import com.google.protobuf.InvalidProtocolBufferException;
import in.xnnyygn.xraft.kvstore.KVStoreProtos;

import java.io.*;

public class SetCommand implements Serializable {

    private final String key;
    private final String value;

    public SetCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public static SetCommand fromBytes(byte[] bytes) {
        try {
            KVStoreProtos.SetCommand protoCommand = KVStoreProtos.SetCommand.parseFrom(bytes);
            return new SetCommand(protoCommand.getKey(), protoCommand.getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("failed to deserialize set command", e);
        }
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public byte[] toBytes() {
        return KVStoreProtos.SetCommand.newBuilder().setKey(this.key).setValue(this.value).build().toByteArray();
    }

    @Override
    public String toString() {
        return "SetCommand{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

}
