package in.xnnyygn.xraft.kvstore;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import in.xnnyygn.xraft.core.log.CommandApplier;
import in.xnnyygn.xraft.core.log.SnapshotApplier;
import in.xnnyygn.xraft.core.log.SnapshotGenerator;
import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import in.xnnyygn.xraft.kvstore.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Service implements CommandApplier, SnapshotGenerator, SnapshotApplier {

    private static final Logger logger = LoggerFactory.getLogger(Service.class);
    private final Node node;
    private Map<String, byte[]> map = new HashMap<>();

    public Service(Node node) {
        this.node = node;
        this.node.setCommandApplier(this);
        this.node.setSnapshotGenerator(this);
        this.node.setSnapshotApplier(this);
    }

    public void set(CommandRequest<SetCommand> setCommandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            setCommandRequest.reply(redirect);
            return;
        }

        logger.info("set {}", setCommandRequest.getCommand().getKey());
        this.node.appendLog(setCommandRequest.getCommand().toBytes(), (index, commandBytes) -> {
            Service.this.applyCommand(index, commandBytes);
            setCommandRequest.reply(Success.INSTANCE);
        });
    }

    public void get(CommandRequest<GetCommand> getCommandCommandRequest) {
        String key = getCommandCommandRequest.getCommand().getKey();
        logger.info("get {}", key);
        byte[] value = this.map.get(key);
        // TODO view from node state machine
        getCommandCommandRequest.reply(new GetCommandResponse(value));
    }

    @Override
    public void applyCommand(int index, byte[] commandBytes) {
        logger.debug("apply command, index {}", index);
        SetCommand command = SetCommand.fromBytes(commandBytes);
        this.map.put(command.getKey(), command.getValue());
    }

    @Override
    public byte[] generateSnapshot() {
        return toSnapshot(this.map);
    }

    @Override
    public void applySnapshot(byte[] snapshot) {
        logger.info("apply snapshot");
        this.map = fromSnapshot(snapshot);
    }

    private Redirect checkLeadership() {
        NodeStateSnapshot state = this.node.getNodeState();
        if (state.getRole() == NodeRole.FOLLOWER) {
            NodeId leaderId = state.getLeaderId();
            return new Redirect(leaderId != null ? leaderId.getValue() : null);
        }

        if (state.getRole() == NodeRole.CANDIDATE) {
            return new Redirect(null);
        }
        return null;
    }

    static byte[] toSnapshot(Map<String, byte[]> map) {
        Protos.EntryList.Builder entryList = Protos.EntryList.newBuilder();
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            entryList.addEntries(
                    Protos.EntryList.Entry.newBuilder()
                            .setKey(entry.getKey())
                            .setValue(ByteString.copyFrom(entry.getValue())).build()
            );
        }
        return entryList.build().toByteArray();
    }

    static Map<String, byte[]> fromSnapshot(byte[] snapshot) {
        Map<String, byte[]> map = new HashMap<>();
        try {
            Protos.EntryList entryList = Protos.EntryList.parseFrom(snapshot);
            for (Protos.EntryList.Entry entry : entryList.getEntriesList()) {
                map.put(entry.getKey(), entry.getValue().toByteArray());
            }
            return map;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("failed to load map from snapshot", e);
        }
    }

}
