package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.log.CommandApplier;
import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import in.xnnyygn.xraft.kvstore.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Service implements CommandApplier {

    private static final Logger logger = LoggerFactory.getLogger(Service.class);
    private final Node node;
    private final Map<String, byte[]> map = new HashMap<>();

    public Service(Node node) {
        this.node = node;
        this.node.setCommandApplier(this);
    }

    public void set(String key, byte[] value) {
        logger.info("set {}", key);
        this.map.put(key, value);
    }

    public byte[] get(String key) {
        logger.info("get {}", key);
        return this.map.get(key);
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

}
