package in.xnnyygn.xraft.kvstore.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.service.*;
import in.xnnyygn.xraft.kvstore.MessageConstants;
import in.xnnyygn.xraft.kvstore.Protos;
import in.xnnyygn.xraft.kvstore.message.GetCommand;
import in.xnnyygn.xraft.kvstore.message.SetCommand;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketChannel implements Channel {

    private final String host;
    private final int port;

    public SocketChannel(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public Object send(Object payload) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(this.host, this.port));
            this.write(socket.getOutputStream(), payload);
            return this.read(socket.getInputStream());
        } catch (IOException e) {
            throw new ChannelException("failed to send and receive", e);
        }
    }

    private Object read(InputStream input) throws IOException {
        DataInputStream dataInput = new DataInputStream(input);
        int messageType = dataInput.readInt();
        int payloadLength = dataInput.readInt();
        byte[] payload = new byte[payloadLength];
        dataInput.readFully(payload);
        switch (messageType) {
            case MessageConstants.MSG_TYPE_SUCCESS:
                return null;
            case MessageConstants.MSG_TYPE_FAILURE:
                Protos.Failure protoFailure = Protos.Failure.parseFrom(payload);
                throw new ChannelException("error code " + protoFailure.getErrorCode() + ", message " + protoFailure.getMessage());
            case MessageConstants.MSG_TYPE_REDIRECT:
                Protos.Redirect protoRedirect = Protos.Redirect.parseFrom(payload);
                String rawLeaderId = protoRedirect.getLeaderId();
                NodeId leaderId = rawLeaderId.isEmpty() ? null : NodeId.of(rawLeaderId);
                throw new RedirectException(leaderId);
            case MessageConstants.MSG_TYPE_GET_COMMAND_RESPONSE:
                Protos.GetCommandResponse protoGetCommandResponse = Protos.GetCommandResponse.parseFrom(payload);
                if (!protoGetCommandResponse.getFound()) return null;
                return protoGetCommandResponse.getValue().toByteArray();
            default:
                throw new ChannelException("unexpected message type " + messageType);
        }
    }

    private void write(OutputStream output, Object payload) throws IOException {
        if (payload instanceof GetCommand) {
            Protos.GetCommand protoGetCommand = Protos.GetCommand.newBuilder().setKey(((GetCommand) payload).getKey()).build();
            this.write(output, MessageConstants.MSG_TYPE_GET_COMMAND, protoGetCommand);
        } else if (payload instanceof SetCommand) {
            SetCommand setCommand = (SetCommand) payload;
            Protos.SetCommand protoSetCommand = Protos.SetCommand.newBuilder()
                    .setKey(setCommand.getKey())
                    .setValue(ByteString.copyFrom(setCommand.getValue())).build();
            this.write(output, MessageConstants.MSG_TYPE_SET_COMMAND, protoSetCommand);
        } else if (payload instanceof AddNodeCommand) {
            AddNodeCommand command = (AddNodeCommand) payload;
            Protos.AddNodeCommand protoAddServerCommand = Protos.AddNodeCommand.newBuilder().setNodeId(command.getNodeId())
                    .setHost(command.getHost()).setPort(command.getPort()).build();
            this.write(output, MessageConstants.MSG_TYPE_ADD_SERVER_COMMAND, protoAddServerCommand);
        } else if (payload instanceof RemoveNodeCommand) {
            RemoveNodeCommand command = (RemoveNodeCommand) payload;
            Protos.RemoveNodeCommand protoRemoveServerCommand = Protos.RemoveNodeCommand.newBuilder().setNodeId(command.getNodeId().getValue()).build();
            this.write(output, MessageConstants.MSG_TYPE_REMOVE_SERVER_COMMAND, protoRemoveServerCommand);
        }
    }

    private void write(OutputStream output, int messageType, MessageLite message) throws IOException {
        DataOutputStream dataOutput = new DataOutputStream(output);
        byte[] messageBytes = message.toByteArray();
        dataOutput.writeInt(messageType);
        dataOutput.writeInt(messageBytes.length);
        dataOutput.write(messageBytes);
        dataOutput.flush();
    }


}
