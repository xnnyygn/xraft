package in.xnnyygn.xraft.kvstore.command;

import java.io.*;

public class SetCommand implements Serializable {

    private final String key;
    private final String value;

    public SetCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public static SetCommand fromBytes(byte[] bytes) {
        try (DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int keyBytesLength = dataInput.readInt();
            byte[] keyBytes = new byte[keyBytesLength];
            dataInput.readFully(keyBytes);

            int valueBytesLength = dataInput.readInt();
            byte[] valueBytes = new byte[valueBytesLength];
            dataInput.readFully(valueBytes);
            return new SetCommand(new String(keyBytes), new String(valueBytes));
        } catch (IOException e) {
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
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            DataOutputStream dataOutput = new DataOutputStream(output);
            byte[] keyBytes = this.key.getBytes();
            dataOutput.writeInt(keyBytes.length);
            dataOutput.write(keyBytes);
            byte[] valueBytes = this.value.getBytes();
            dataOutput.writeInt(valueBytes.length);
            dataOutput.write(valueBytes);
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize set command", e);
        }
    }

    @Override
    public String toString() {
        return "SetCommand{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

}
