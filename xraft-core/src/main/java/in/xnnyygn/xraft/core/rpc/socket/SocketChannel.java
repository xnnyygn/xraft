package in.xnnyygn.xraft.core.rpc.socket;

import com.google.common.eventbus.EventBus;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import in.xnnyygn.xraft.core.log.Entry;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.ChannelException;
import in.xnnyygn.xraft.core.rpc.DirectionalChannel;
import in.xnnyygn.xraft.core.rpc.Protos;
import in.xnnyygn.xraft.core.rpc.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static in.xnnyygn.xraft.core.rpc.message.MessageConstants.*;

public class SocketChannel implements DirectionalChannel {

    private static final Logger logger = LoggerFactory.getLogger(SocketChannel.class);

    private final Socket socket;
    private final Direction direction;
    private final EventBus eventBus;
    private final ExecutorService executorService;
    private final SocketChannelContext channelContext;
    private NodeId remoteId;
    private String name;
    private AppendEntriesRpc lastAppendEntriesRpc;

    public SocketChannel(Socket socket, Direction direction, EventBus eventBus, ExecutorService executorService, SocketChannelContext channelContext) {
        this.socket = socket;
        this.direction = direction;
        this.eventBus = eventBus;
        this.executorService = executorService;
        this.channelContext = channelContext;
    }

    public Direction getDirection() {
        return this.direction;
    }

    public NodeId getRemoteId() {
        return this.remoteId;
    }

    public void setRemoteId(NodeId remoteId) {
        this.remoteId = remoteId;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void open(NodeId localId) {
        this.sendLocalId(localId);
        logger.debug("open channel to node {}", this.remoteId);
        this.name = "socket-channel-outbound-" + this.remoteId;
    }

    private void sendLocalId(NodeId localId) {
        logger.debug("send local id {} to remote node {}", localId, this.remoteId);
        try {
            DataOutputStream dataOutput = new DataOutputStream(this.socket.getOutputStream());
            byte[] localIdBytes = localId.getValue().getBytes();
            dataOutput.writeByte(localIdBytes.length);
            dataOutput.write(localIdBytes);
            dataOutput.flush();
        } catch (IOException e) {
            throw new ChannelException("failed to send local id", e);
        }
    }

    public void open() {
        this.remoteId = this.receiveRemoteId();
        logger.debug("open channel from node {}", this.remoteId);
        this.name = "socket-channel-inbound-" + this.remoteId;
        this.executorService.submit(this::readAndAction);
    }

    private NodeId receiveRemoteId() {
        logger.debug("wait for id from remote node");
        try {
            DataInputStream dataInput = new DataInputStream(this.socket.getInputStream());
            int remoteIdLength = dataInput.readByte();
            byte[] remoteIdBytes = new byte[remoteIdLength];
            dataInput.readFully(remoteIdBytes);
            return new NodeId(new String(remoteIdBytes));
        } catch (IOException e) {
            throw new ChannelException("failed to receive remote id", e);
        }
    }

    private void readAndAction() {
        // socket is not closed
        try {
            this.doReadAndAction();
        } catch (IOException e) {
            logger.warn("failed to read, cause " + e.getMessage());
            if (!this.socket.isClosed()) {
                this.channelContext.closeChannel(this);
            }
        }
    }

    private void doReadAndAction() throws IOException {
        logger.debug("wait message from {}", this.remoteId);
        InputStream input = this.socket.getInputStream();
        int messageType = input.read();
        if (messageType < 0) {
            logger.debug("receive EOF from {}", this.remoteId);
            this.channelContext.closeChannel(this);
            return;
        }

        switch (messageType) {
            case MSG_TYPE_REQUEST_VOTE_RPC:
                Protos.RequestVoteRpc protoRVRpc = Protos.RequestVoteRpc.parseDelimitedFrom(input);
                RequestVoteRpc rpc = new RequestVoteRpc();
                rpc.setTerm(protoRVRpc.getTerm());
                rpc.setCandidateId(new NodeId(protoRVRpc.getCandidateId()));
                rpc.setLastLogIndex(protoRVRpc.getLastLogIndex());
                rpc.setLastLogTerm(protoRVRpc.getLastLogTerm());
                this.eventBus.post(new RequestVoteRpcMessage(rpc, this.remoteId, this));
                break;
            case MSG_TYPE_REQUEST_VOTE_RESULT:
                Protos.RequestVoteResult protoRVResult = Protos.RequestVoteResult.parseDelimitedFrom(input);
                this.eventBus.post(new RequestVoteResult(protoRVResult.getTerm(), protoRVResult.getVoteGranted()));
                break;
            case MSG_TYPE_APPEND_ENTRIES_RPC:
                Protos.AppendEntriesRpc protoAERpc = Protos.AppendEntriesRpc.parseDelimitedFrom(input);
                AppendEntriesRpc aeRpc = new AppendEntriesRpc();
                aeRpc.setTerm(protoAERpc.getTerm());
                aeRpc.setLeaderId(new NodeId(protoAERpc.getLeaderId()));
                aeRpc.setLeaderCommit(protoAERpc.getLeaderCommit());
                aeRpc.setPrevLogIndex(protoAERpc.getPrevLogIndex());
                aeRpc.setPrevLogTerm(protoAERpc.getPrevLogTerm());
                aeRpc.setEntries(protoAERpc.getEntriesList().stream().map(e ->
                        new Entry(e.getIndex(), e.getTerm(), e.getCommand().toByteArray())
                ).collect(Collectors.toList()));
                this.eventBus.post(new AppendEntriesRpcMessage(aeRpc, this.remoteId, this));
                break;
            case MSG_TYPE_APPEND_ENTRIES_RESULT:
                Protos.AppendEntriesResult protoAEResult = Protos.AppendEntriesResult.parseDelimitedFrom(input);
                AppendEntriesResult aeResult = new AppendEntriesResult(protoAEResult.getTerm(), protoAEResult.getSuccess());
                this.eventBus.post(new AppendEntriesResultMessage(aeResult, this.remoteId, this.lastAppendEntriesRpc));
                this.lastAppendEntriesRpc = null;
                break;
            default:
                throw new IllegalStateException("unknown message type " + messageType);
        }
    }

    @Override
    public void writeRequestVoteRpc(RequestVoteRpc rpc, NodeId senderId) {
        Protos.RequestVoteRpc protoRpc = Protos.RequestVoteRpc.newBuilder()
                .setTerm(rpc.getTerm())
                .setCandidateId(rpc.getCandidateId().getValue())
                .setLastLogIndex(rpc.getLastLogIndex())
                .setLastLogTerm(rpc.getLastLogTerm())
                .build();
        this.writeMessage(MSG_TYPE_REQUEST_VOTE_RPC, protoRpc, rpc);
    }

    @Override
    public void writeRequestVoteResult(RequestVoteResult result, NodeId senderId, RequestVoteRpc rpc) {
        Protos.RequestVoteResult protoResult = Protos.RequestVoteResult.newBuilder()
                .setTerm(result.getTerm())
                .setVoteGranted(result.isVoteGranted())
                .build();
        this.writeMessage(MSG_TYPE_REQUEST_VOTE_RESULT, protoResult, result);
    }

    @Override
    public void writeAppendEntriesRpc(AppendEntriesRpc rpc, NodeId senderId) {
        Protos.AppendEntriesRpc protoRpc = Protos.AppendEntriesRpc.newBuilder()
                .setTerm(rpc.getTerm())
                .setLeaderId(rpc.getLeaderId().getValue())
                .setLeaderCommit(rpc.getLeaderCommit())
                .setPrevLogIndex(rpc.getPrevLogIndex())
                .setPrevLogTerm(rpc.getPrevLogTerm())
                .addAllEntries(
                        rpc.getEntries().stream().map(e ->
                                Protos.AppendEntriesRpc.Entry.newBuilder()
                                        .setIndex(e.getIndex())
                                        .setTerm(e.getTerm())
                                        .setCommand(ByteString.copyFrom(e.getCommand()))
                                        .build()
                        ).collect(Collectors.toList())
                ).build();
        this.writeMessage(MSG_TYPE_APPEND_ENTRIES_RPC, protoRpc, rpc);
    }

    @Override
    public void writeAppendEntriesResult(AppendEntriesResult result, NodeId senderId, AppendEntriesRpc rpc) {
        Protos.AppendEntriesResult protoResult = Protos.AppendEntriesResult.newBuilder()
                .setTerm(result.getTerm())
                .setSuccess(result.isSuccess())
                .build();
        this.writeMessage(MSG_TYPE_APPEND_ENTRIES_RESULT, protoResult, result);
    }


    private void writeMessage(int messageType, MessageLite message, Object originalMessage) {
        this.executorService.submit(() -> {
            if (this.doWriteMessage(messageType, message)) {
                this.saveLastMessage(messageType, originalMessage);
                this.readAndAction();
            }
        });
    }

    private void saveLastMessage(int messageType, Object originalMessage) {
        if (messageType == MSG_TYPE_APPEND_ENTRIES_RPC) {
            this.lastAppendEntriesRpc = (AppendEntriesRpc) originalMessage;
        }
    }

    private boolean doWriteMessage(int messageType, MessageLite message) {
        logger.debug("send message to node {}", this.remoteId);
        try {
            OutputStream output = this.socket.getOutputStream();
            output.write(messageType);
            message.writeDelimitedTo(output);
            output.flush();
            return true;
        } catch (IOException e) {
            logger.warn("failed to write message, cause " + e.getMessage());
            if (!this.socket.isClosed()) {
                this.channelContext.closeChannel(this);
            }
            return false;
        }
    }

    @Override
    public void close() {
        logger.debug("close {}", this.name);
        try {
            this.socket.close();
        } catch (IOException e) {
            throw new ChannelException("failed to close socket", e);
        }
    }

    @Override
    public String toString() {
        return "SocketChannel{" + name + '}';
    }

}
