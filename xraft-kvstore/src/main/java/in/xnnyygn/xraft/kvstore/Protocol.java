package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.service.NodeStateException;
import in.xnnyygn.xraft.kvstore.command.GetCommand;
import in.xnnyygn.xraft.kvstore.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Protocol {

    private static final Logger logger = LoggerFactory.getLogger(Protocol.class);

    public static final Object PAYLOAD_VOID = new Object();
    public static final Object PAYLOAD_QUIT = "quit";

    private static final int MSG_VOID = 1;
    private static final int MSG_QUIT = 2;
    private static final int TYPE_STRING = 10;
    private static final int TYPE_SET_COMMAND = 100;
    private static final int TYPE_GET_COMMAND = 101;
    private static final int TYPE_NODE_STATE_EXCEPTION = 102;

    private static final int LEN_NULL_STRING = -1;


    public void toWire(Object payload, OutputStream output) throws IOException {
        logger.debug("to wire, payload " + payload);
        DataOutputStream dataOutput = new DataOutputStream(output);
        if (payload == PAYLOAD_VOID) {
            dataOutput.writeInt(MSG_VOID);
        } else if (payload == PAYLOAD_QUIT) {
            dataOutput.writeInt(MSG_QUIT);
        } else if (payload instanceof String) {
            dataOutput.writeInt(TYPE_STRING);
            byte[] bytes = ((String) payload).getBytes();
            dataOutput.writeInt(bytes.length);
            dataOutput.write(bytes);
        } else if (payload instanceof GetCommand) {
            dataOutput.writeInt(TYPE_GET_COMMAND);
            byte[] keyBytes = ((GetCommand) payload).getKey().getBytes();
            dataOutput.writeInt(keyBytes.length);
            dataOutput.write(keyBytes);
        } else if (payload instanceof SetCommand) {
            SetCommand command = (SetCommand) payload;
            dataOutput.writeInt(TYPE_SET_COMMAND);
            byte[] keyBytes = command.getKey().getBytes();
            dataOutput.writeInt(keyBytes.length);
            dataOutput.write(keyBytes);
            byte[] valueBytes = command.getValue().getBytes();
            dataOutput.writeInt(valueBytes.length);
            dataOutput.write(valueBytes);
        } else if (payload instanceof NodeStateException) {
            NodeStateException exception = (NodeStateException) payload;
            dataOutput.writeInt(TYPE_NODE_STATE_EXCEPTION);
            byte[] roleBytes = exception.getRole().name().getBytes();
            dataOutput.writeInt(roleBytes.length);
            dataOutput.write(roleBytes);
            NodeId leaderId = exception.getLeaderId();
            if (leaderId != null) {
                byte[] leaderIdBytes = exception.getLeaderId().getValue().getBytes();
                dataOutput.writeInt(leaderIdBytes.length);
                dataOutput.write(leaderIdBytes);
            } else {
                dataOutput.writeInt(LEN_NULL_STRING);
            }
        }
        dataOutput.flush();
    }

    public Object fromWire(InputStream input) throws IOException {
        DataInputStream dataInput = new DataInputStream(input);
        switch (dataInput.readInt()) {
            case MSG_VOID:
                return null;
            case MSG_QUIT:
                return PAYLOAD_QUIT;
            case TYPE_STRING:
                return readString(dataInput);
            case TYPE_GET_COMMAND:
                return new GetCommand(readString(dataInput));
            case TYPE_SET_COMMAND:
                String key = readString(dataInput);
                String value = readString(dataInput);
                return new SetCommand(key, value);
            case TYPE_NODE_STATE_EXCEPTION:
                NodeRole role = NodeRole.valueOf(readString(dataInput));
                String nodeIdValue = readString(dataInput);
                NodeId leaderId = nodeIdValue != null ? new NodeId(nodeIdValue) : null;
                return new NodeStateException(role, leaderId);
            default:
                throw new IllegalArgumentException("unexpected input type " + input.getClass());
        }
    }

    private String readString(DataInputStream dataInput) throws IOException {
        int length = dataInput.readInt();
        if (length == LEN_NULL_STRING) return null;

        byte[] buffer = new byte[length];
        dataInput.readFully(buffer);
        return new String(buffer);
    }

}
