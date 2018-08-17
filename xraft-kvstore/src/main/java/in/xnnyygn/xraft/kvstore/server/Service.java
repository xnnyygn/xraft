package in.xnnyygn.xraft.kvstore.server;

import com.google.protobuf.ByteString;
import in.xnnyygn.xraft.core.log.StateMachine;
import in.xnnyygn.xraft.core.node.task.GroupConfigChangeTaskReference;
import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.node.role.RoleName;
import in.xnnyygn.xraft.core.node.role.RoleNameAndLeaderId;
import in.xnnyygn.xraft.core.service.AddNodeCommand;
import in.xnnyygn.xraft.core.service.RemoveNodeCommand;
import in.xnnyygn.xraft.kvstore.Protos;
import in.xnnyygn.xraft.kvstore.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

public class Service implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(Service.class);
    private final Node node;
    private final ConcurrentMap<String, CommandRequest<?>> pendingCommands = new ConcurrentHashMap<>();
    private Map<String, byte[]> map = new HashMap<>();

    public Service(Node node) {
        this.node = node;
        this.node.registerStateMachine(this);
    }

    public void addNode(CommandRequest<AddNodeCommand> commandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            commandRequest.reply(redirect);
            return;
        }

        AddNodeCommand command = commandRequest.getCommand();
        GroupConfigChangeTaskReference taskReference = this.node.addNode(command.toNodeEndpoint());
        awaitResult(taskReference, commandRequest);
    }

    private <T> void awaitResult(GroupConfigChangeTaskReference taskReference, CommandRequest<T> commandRequest) {
        try {
            switch (taskReference.getResult(3000L)) {
                case OK:
                    commandRequest.reply(Success.INSTANCE);
                    break;
                case TIMEOUT:
                    commandRequest.reply(new Failure(101, "timeout"));
                    break;
                default:
                    commandRequest.reply(new Failure(100, "error"));
            }
        } catch (TimeoutException e) {
            commandRequest.reply(new Failure(101, "timeout"));
        } catch (InterruptedException ignored) {
            commandRequest.reply(new Failure(100, "error"));
        }
    }

    public void removeNode(CommandRequest<RemoveNodeCommand> commandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            commandRequest.reply(redirect);
            return;
        }

        RemoveNodeCommand command = commandRequest.getCommand();
        GroupConfigChangeTaskReference taskReference = node.removeNode(command.getNodeId());
        awaitResult(taskReference, commandRequest);
    }

    public void set(CommandRequest<SetCommand> commandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            commandRequest.reply(redirect);
            return;
        }

        SetCommand command = commandRequest.getCommand();
        logger.info("set {}", command.getKey());
        this.pendingCommands.put(command.getRequestId(), commandRequest);
        commandRequest.addCloseListener(() -> pendingCommands.remove(command.getRequestId()));
        this.node.appendLog(command.toBytes());
    }

    public void get(CommandRequest<GetCommand> commandRequest) {
        String key = commandRequest.getCommand().getKey();
        logger.info("get {}", key);
        byte[] value = this.map.get(key);
        // TODO view from node state machine
        commandRequest.reply(new GetCommandResponse(value));
    }

    @Override
    public void applyLog(int index, byte[] commandBytes) {
        SetCommand command = SetCommand.fromBytes(commandBytes);
        map.put(command.getKey(), command.getValue());
        CommandRequest<?> commandRequest = pendingCommands.remove(command.getRequestId());
        if (commandRequest != null) {
            commandRequest.reply(Success.INSTANCE);
        }
    }

    @Override
    public void generateSnapshot(OutputStream output) throws IOException {
        toSnapshot(map, output);
    }

    @Override
    public void applySnapshot(InputStream input) throws IOException {
        logger.info("apply snapshot");
        this.map = fromSnapshot(input);
    }

    private Redirect checkLeadership() {
        RoleNameAndLeaderId state = node.getRoleNameAndLeaderId();
        if (state.getRoleName() != RoleName.LEADER) {
            return new Redirect(state.getLeaderId());
        }
        return null;
    }

    static void toSnapshot(Map<String, byte[]> map, OutputStream output) throws IOException {
        Protos.EntryList.Builder entryList = Protos.EntryList.newBuilder();
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            entryList.addEntries(
                    Protos.EntryList.Entry.newBuilder()
                            .setKey(entry.getKey())
                            .setValue(ByteString.copyFrom(entry.getValue())).build()
            );
        }
        entryList.build().writeTo(output);
        entryList.build().getSerializedSize();
    }

    static Map<String, byte[]> fromSnapshot(InputStream input) throws IOException {
        Map<String, byte[]> map = new HashMap<>();
        Protos.EntryList entryList = Protos.EntryList.parseFrom(input);
        for (Protos.EntryList.Entry entry : entryList.getEntriesList()) {
            map.put(entry.getKey(), entry.getValue().toByteArray());
        }
        return map;
    }

}
