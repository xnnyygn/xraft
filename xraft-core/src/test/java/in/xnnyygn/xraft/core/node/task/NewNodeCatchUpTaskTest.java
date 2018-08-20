package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.config.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.support.SingleThreadTaskExecutor;
import in.xnnyygn.xraft.core.support.TaskExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NewNodeCatchUpTaskTest {

    private static TaskExecutor taskExecutor;

    @BeforeClass
    public static void beforeClass() {
        taskExecutor = new SingleThreadTaskExecutor();
    }

    @Test
    public void testNoResponseWithinTimeout() throws Exception {
        NodeConfig config = new NodeConfig();
        config.setNewNodeReadTimeout(1);
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                new WaitableNewNodeCatchUpTaskContext(),
                new NodeEndpoint("D", "localhost", 2336),
                config
        );
        NewNodeCatchUpTaskResult result = task.call();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.TIMEOUT, result.getState());
    }

    @Test
    public void testReplicationError() throws ExecutionException, InterruptedException {
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        task.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, false),
                        NodeId.of("D"),
                        new AppendEntriesRpc()
                ),
                1
        );
        NewNodeCatchUpTaskResult result = future.get();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.REPLICATION_FAILED, result.getState());
    }

    @Test
    public void testCannotMakeProgressWithinTimeout() throws ExecutionException, InterruptedException {
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NodeConfig config = new NodeConfig();
        config.setNewNodeAdvanceTimeout(0);
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                config
        );
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        task.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, false),
                        NodeId.of("D"),
                        new AppendEntriesRpc()
                ),
                2
        );
        NewNodeCatchUpTaskResult result = future.get();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.TIMEOUT, result.getState());
    }

    private AppendEntriesRpc createAppendEntriesRpc(int lastEntryIndex) {
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setPrevLogIndex(lastEntryIndex);
        return rpc;
    }

    @Test
    public void testExceedMaxRound() throws ExecutionException, InterruptedException {
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NodeConfig config = new NodeConfig();
        config.setNewNodeMaxRound(2);
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                config
        );
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        task.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, true),
                        NodeId.of("D"),
                        createAppendEntriesRpc(1)
                ),
                10
        );
        task.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, true),
                        NodeId.of("D"),
                        createAppendEntriesRpc(2)
                ),
                10
        );
        NewNodeCatchUpTaskResult result = future.get();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.TIMEOUT, result.getState());
    }

    @Test
    public void testNormal() throws ExecutionException, InterruptedException {
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        task.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, true),
                        NodeId.of("D"),
                        createAppendEntriesRpc(1)
                ),
                3
        );
        task.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, true),
                        NodeId.of("D"),
                        createAppendEntriesRpc(2)
                ),
                3
        );
        NewNodeCatchUpTaskResult result = future.get();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.OK, result.getState());
        Assert.assertEquals(3, result.getNextIndex());
        Assert.assertEquals(2, result.getMatchIndex());
    }

    @Test(expected = IllegalStateException.class)
    public void testInstallSnapshotIllegalState() {
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                new WaitableNewNodeCatchUpTaskContext(),
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        task.onReceiveInstallSnapshotResult(new InstallSnapshotResultMessage(
                new InstallSnapshotResult(1),
                NodeId.of("D"),
                new InstallSnapshotRpc()
        ), 1);
    }

    @Test
    public void testInstallSnapshotCatchUp() throws InterruptedException, ExecutionException {
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setLastIndex(2);
        rpc.setData(new byte[0]);
        rpc.setDone(true);
        task.onReceiveInstallSnapshotResult(new InstallSnapshotResultMessage(
                new InstallSnapshotResult(1),
                NodeId.of("D"),
                rpc
        ), 3);
        NewNodeCatchUpTaskResult result = future.get();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.OK, result.getState());
        Assert.assertEquals(2, result.getMatchIndex());
        Assert.assertEquals(3, result.getNextIndex());
    }

    // catch up in 2 rpc
    @Test
    public void testInstallSnapshotCatchUp2() throws InterruptedException, ExecutionException {
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setLastIndex(2);
        rpc.setData(new byte[0]);
        task.onReceiveInstallSnapshotResult(new InstallSnapshotResultMessage(
                new InstallSnapshotResult(1),
                NodeId.of("D"),
                rpc
        ), 3);
        rpc.setDone(true);
        task.onReceiveInstallSnapshotResult(new InstallSnapshotResultMessage(
                new InstallSnapshotResult(1),
                NodeId.of("D"),
                rpc
        ), 3);
        NewNodeCatchUpTaskResult result = future.get();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.OK, result.getState());
        Assert.assertEquals(2, result.getMatchIndex());
        Assert.assertEquals(3, result.getNextIndex());
    }

    // catch up by install snapshot + append entries
    @Test
    public void testInstallSnapshotCatchUp3() throws InterruptedException, ExecutionException {
        WaitableNewNodeCatchUpTaskContext taskContext = new WaitableNewNodeCatchUpTaskContext();
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        Future<NewNodeCatchUpTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitReplicateLog();
        InstallSnapshotRpc installSnapshotRpc = new InstallSnapshotRpc();
        installSnapshotRpc.setLastIndex(2);
        installSnapshotRpc.setData(new byte[0]);
        installSnapshotRpc.setDone(true);
        task.onReceiveInstallSnapshotResult(new InstallSnapshotResultMessage(
                new InstallSnapshotResult(1),
                NodeId.of("D"),
                installSnapshotRpc
        ), 4);
        AppendEntriesRpc appendEntriesRpc = new AppendEntriesRpc();
        appendEntriesRpc.setPrevLogIndex(3);
        task.onReceiveAppendEntriesResult(new AppendEntriesResultMessage(
                new AppendEntriesResult("", 1, true),
                NodeId.of("D"),
                appendEntriesRpc
        ), 4);
        NewNodeCatchUpTaskResult result = future.get();
        Assert.assertEquals(NewNodeCatchUpTaskResult.State.OK, result.getState());
        Assert.assertEquals(3, result.getMatchIndex());
        Assert.assertEquals(4, result.getNextIndex());
    }

    @Test(expected = IllegalStateException.class)
    public void testOnReceiveAppendEntriesResultNotReplicating() {
        NewNodeCatchUpTask task = new NewNodeCatchUpTask(
                new WaitableNewNodeCatchUpTaskContext(),
                new NodeEndpoint("D", "localhost", 2336),
                new NodeConfig()
        );
        task.onReceiveAppendEntriesResult(
                new AppendEntriesResultMessage(
                        new AppendEntriesResult("", 1, true),
                        NodeId.of("D"),
                        new AppendEntriesRpc()
                ),
                1
        );
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        taskExecutor.shutdown();
    }

}