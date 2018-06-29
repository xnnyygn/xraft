package in.xnnyygn.xraft.kvstore.message;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import in.xnnyygn.xraft.kvstore.Protos;

public class SetCommand {

    private final String key;
    private final byte[] value;

    public SetCommand(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public static SetCommand fromBytes(byte[] bytes) {
        try {
            Protos.SetCommand protoCommand = Protos.SetCommand.parseFrom(bytes);
            return new SetCommand(protoCommand.getKey(), protoCommand.getValue().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("failed to deserialize set command", e);
        }
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public byte[] toBytes() {
        return Protos.SetCommand.newBuilder().setKey(this.key).setValue(ByteString.copyFrom(this.value)).build().toByteArray();
    }

    @Override
    public String toString() {
        return "SetCommand{" + "key='" + key + '}';
    }

}
