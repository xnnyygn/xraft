package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.service.NodeStateException;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class ProtocolTest {

    @Test
    public void test() throws IOException {
        Protocol protocol = new Protocol();
        NodeStateException exception = new NodeStateException(NodeRole.FOLLOWER, new NodeId("B"));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        protocol.toWire(exception, output);
        System.out.println(Arrays.toString(output.toByteArray()));
        System.out.println(protocol.fromWire(new ByteArrayInputStream(output.toByteArray())));
    }
}
