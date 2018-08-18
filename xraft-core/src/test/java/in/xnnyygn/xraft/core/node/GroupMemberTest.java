package in.xnnyygn.xraft.core.node;

import org.junit.Assert;
import org.junit.Test;

public class GroupMemberTest {

    @Test
    public void testShouldReplicate() {
        GroupMember member = new GroupMember(new NodeEndpoint("A", "localhost", 2333));
        member.setReplicatingState(new ReplicatingState(10));
        Assert.assertTrue(member.shouldReplicate(1000));
        member.replicateNow();
        Assert.assertFalse(member.shouldReplicate(1000));
        Assert.assertTrue(member.shouldReplicate(0));
        member.stopReplicating();
        Assert.assertTrue(member.shouldReplicate(1000));
    }

}