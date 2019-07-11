package in.xnnyygn.xraft.core.rpc.nio;

import in.xnnyygn.xraft.core.Protos;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.MessageConstants;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;

public class EncoderTest {
    @Test
    public void testNodeId() throws Exception {
        Encoder encoder = new Encoder();
        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, NodeId.of("A"), buffer);
        assertEquals(MessageConstants.MSG_TYPE_NODE_ID, buffer.readInt());
        assertEquals(1, buffer.readInt());
        assertEquals((byte) 'A', buffer.readByte());
    }

    @Test
    public void testRequestVoteRpc() throws Exception {
        Encoder encoder = new Encoder();
        ByteBuf buffer = Unpooled.buffer();
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setLastLogIndex(2);
        rpc.setLastLogTerm(1);
        rpc.setTerm(2);
        rpc.setCandidateId(NodeId.of("A"));
        encoder.encode(null, rpc, buffer);
        assertEquals(MessageConstants.MSG_TYPE_REQUEST_VOTE_RPC, buffer.readInt());
        buffer.readInt(); // skip length
        Protos.RequestVoteRpc decodedRpc = Protos.RequestVoteRpc.parseFrom(new ByteBufInputStream(buffer));
        assertEquals(rpc.getLastLogIndex(), decodedRpc.getLastLogIndex());
        assertEquals(rpc.getLastLogTerm(), decodedRpc.getLastLogTerm());
        assertEquals(rpc.getTerm(), decodedRpc.getTerm());
        assertEquals(rpc.getCandidateId().getValue(), decodedRpc.getCandidateId());
    }
}