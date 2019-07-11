package in.xnnyygn.xraft.core.rpc.nio;

import in.xnnyygn.xraft.core.Protos;
import in.xnnyygn.xraft.core.log.entry.EntryFactory;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.stream.Collectors;

public class Decoder extends ByteToMessageDecoder {

    private final EntryFactory entryFactory = new EntryFactory();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int availableBytes = in.readableBytes();
        if (availableBytes < 8) return;

        in.markReaderIndex();
        int messageType = in.readInt();
        int payloadLength = in.readInt();
        if (in.readableBytes() < payloadLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] payload = new byte[payloadLength];
        in.readBytes(payload);
        switch (messageType) {
            case MessageConstants.MSG_TYPE_NODE_ID:
                out.add(new NodeId(new String(payload)));
                break;
            case MessageConstants.MSG_TYPE_REQUEST_VOTE_RPC:
                Protos.RequestVoteRpc protoRVRpc = Protos.RequestVoteRpc.parseFrom(payload);
                RequestVoteRpc rpc = new RequestVoteRpc();
                rpc.setTerm(protoRVRpc.getTerm());
                rpc.setCandidateId(new NodeId(protoRVRpc.getCandidateId()));
                rpc.setLastLogIndex(protoRVRpc.getLastLogIndex());
                rpc.setLastLogTerm(protoRVRpc.getLastLogTerm());
                out.add(rpc);
                break;
            case MessageConstants.MSG_TYPE_REQUEST_VOTE_RESULT:
                Protos.RequestVoteResult protoRVResult = Protos.RequestVoteResult.parseFrom(payload);
                out.add(new RequestVoteResult(protoRVResult.getTerm(), protoRVResult.getVoteGranted()));
                break;
            case MessageConstants.MSG_TYPE_APPEND_ENTRIES_RPC:
                Protos.AppendEntriesRpc protoAERpc = Protos.AppendEntriesRpc.parseFrom(payload);
                AppendEntriesRpc aeRpc = new AppendEntriesRpc();
                aeRpc.setMessageId(protoAERpc.getMessageId());
                aeRpc.setTerm(protoAERpc.getTerm());
                aeRpc.setLeaderId(new NodeId(protoAERpc.getLeaderId()));
                aeRpc.setLeaderCommit(protoAERpc.getLeaderCommit());
                aeRpc.setPrevLogIndex(protoAERpc.getPrevLogIndex());
                aeRpc.setPrevLogTerm(protoAERpc.getPrevLogTerm());
                aeRpc.setEntries(protoAERpc.getEntriesList().stream().map(e ->
                        entryFactory.create(e.getKind(), e.getIndex(), e.getTerm(), e.getCommand().toByteArray())
                ).collect(Collectors.toList()));
                out.add(aeRpc);
                break;
            case MessageConstants.MSG_TYPE_APPEND_ENTRIES_RESULT:
                Protos.AppendEntriesResult protoAEResult = Protos.AppendEntriesResult.parseFrom(payload);
                out.add(new AppendEntriesResult(protoAEResult.getRpcMessageId(), protoAEResult.getTerm(), protoAEResult.getSuccess()));
                break;
            case MessageConstants.MSG_TYPE_INSTALL_SNAPSHOT_PRC:
                Protos.InstallSnapshotRpc protoISRpc = Protos.InstallSnapshotRpc.parseFrom(payload);
                InstallSnapshotRpc isRpc = new InstallSnapshotRpc();
                isRpc.setTerm(protoISRpc.getTerm());
                isRpc.setLeaderId(new NodeId(protoISRpc.getLeaderId()));
                isRpc.setLastIndex(protoISRpc.getLastIndex());
                isRpc.setLastTerm(protoISRpc.getTerm());
                isRpc.setLastConfig(protoISRpc.getLastConfigList().stream().map(e ->
                        new NodeEndpoint(e.getId(), e.getHost(), e.getPort())
                ).collect(Collectors.toSet()));
                isRpc.setOffset(protoISRpc.getOffset());
                isRpc.setData(protoISRpc.getData().toByteArray());
                isRpc.setDone(protoISRpc.getDone());
                out.add(isRpc);
                break;
            case MessageConstants.MSG_TYPE_INSTALL_SNAPSHOT_RESULT:
                Protos.InstallSnapshotResult protoISResult = Protos.InstallSnapshotResult.parseFrom(payload);
                out.add(new InstallSnapshotResult(protoISResult.getTerm()));
                break;
            case MessageConstants.MSG_TYPE_PRE_VOTE_RPC:
                Protos.PreVoteRpc protoPreVoteRpc = Protos.PreVoteRpc.parseFrom(payload);
                PreVoteRpc preVoteRpc = new PreVoteRpc();
                preVoteRpc.setTerm(protoPreVoteRpc.getTerm());
                preVoteRpc.setLastLogIndex(protoPreVoteRpc.getLastLogIndex());
                preVoteRpc.setLastLogTerm(protoPreVoteRpc.getLastLogTerm());
                out.add(preVoteRpc);
                break;
            case MessageConstants.MSG_TYPE_PRE_VOTE_RESULT:
                Protos.PreVoteResult protoPreVoteResult = Protos.PreVoteResult.parseFrom(payload);
                out.add(new PreVoteResult(protoPreVoteResult.getTerm(), protoPreVoteResult.getVoteGranted()));
                break;
        }
    }

}
