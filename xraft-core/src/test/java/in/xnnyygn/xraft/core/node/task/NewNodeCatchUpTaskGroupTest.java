package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResultMessage;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.support.ListeningTaskExecutor;
import in.xnnyygn.xraft.core.support.SingleThreadTaskExecutor;
import in.xnnyygn.xraft.core.support.TaskExecutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class NewNodeCatchUpTaskGroupTest {

    @Test
    public void testAdd() {
        NewNodeCatchUpTaskGroup group = new NewNodeCatchUpTaskGroup();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                new WaitableNewNodeCatchUpTaskContext(),
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        Assert.assertTrue(group.add(task));
    }

    // task for same node exists
    @Test
    public void testAdd2() {
        NewNodeCatchUpTaskGroup group = new NewNodeCatchUpTaskGroup();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                new WaitableNewNodeCatchUpTaskContext(),
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        Assert.assertTrue(group.add(task));
        Assert.assertFalse(group.add(task));
    }

    @Test
    public void testOnReceiveAppendEntriesResultNotFound() {
        NewNodeCatchUpTaskGroup group = new NewNodeCatchUpTaskGroup();
        Assert.assertFalse(group.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, true),
                        NodeId.of("A"),
                        new AppendEntriesRpc()
                ),
                10
        ));
    }

    @Test
    public void testOnReceiveAppendEntriesResult() throws InterruptedException {
        NewNodeCatchUpTaskGroup group = new NewNodeCatchUpTaskGroup();
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        group.add(task);
        TaskExecutor taskExecutor = new SingleThreadTaskExecutor();
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        Assert.assertTrue(group.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, true),
                        NodeId.of("D"),
                        new AppendEntriesRpc()
                ),
                10
        ));
        future.cancel(true);
        taskExecutor.shutdown();
    }

    @Test
    public void testRemove() {
        NewNodeCatchUpTaskGroup group = new NewNodeCatchUpTaskGroup();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                new WaitableNewNodeCatchUpTaskContext(),
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        group.add(task);
        Assert.assertTrue(group.remove(task));
    }

}