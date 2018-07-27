package in.xnnyygn.xraft.core.log.entry;

import com.google.protobuf.InvalidProtocolBufferException;
import in.xnnyygn.xraft.core.log.Protos;

public class EntryFactory {

    public Entry create(int kind, int index, int term, byte[] commandBytes) {
        switch (kind) {
            case Entry.KIND_NO_OP:
                return new NoOpEntry(index, term);
            case Entry.KIND_GENERAL:
                return new GeneralEntry(index, term, commandBytes);
            case Entry.KIND_GROUP_CONFIG:
                try {
                    return new GroupConfigEntry(index, term, Protos.GroupConfigCommand.parseFrom(commandBytes));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException("failed to parse membership change command", e);
                }
            default:
                throw new IllegalArgumentException("unexpected entry kind " + kind);
        }
    }

}
