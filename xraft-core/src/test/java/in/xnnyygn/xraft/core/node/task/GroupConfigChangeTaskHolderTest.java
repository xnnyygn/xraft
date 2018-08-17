package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.log.entry.AddNodeEntry;
import in.xnnyygn.xraft.core.log.entry.RemoveNodeEntry;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.support.ListeningTaskExecutor;
import in.xnnyygn.xraft.core.support.TaskExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GroupConfigChangeTaskHolderTest {

    private static TaskExecutor taskExecutor;

    @BeforeClass
    public static void beforeClass() {
        taskExecutor = new ListeningTaskExecutor(Executors.newSingleThreadExecutor());
    }

    @Test
    public void testIsEmpty() {
        GroupConfigChangeTaskHolder taskHolder = new GroupConfigChangeTaskHolder();
        Assert.assertTrue(taskHolder.isEmpty());
    }

    // not empty
    @Test
    public void testIsEmpty2() {
        GroupConfigChangeTaskHolder taskHolder = new GroupConfigChangeTaskHolder(
                new RemoveNodeTask(new WaitableGroupConfigChangeTaskContext(), NodeId.of("B")),
                new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK)
        );
        Assert.assertFalse(taskHolder.isEmpty());
    }

    @Test
    public void testOnLogCommittedEmpty() {
        GroupConfigChangeTaskHolder taskHolder = new GroupConfigChangeTaskHolder();
        RemoveNodeEntry entry = new RemoveNodeEntry(1, 1, Collections.emptySet(), NodeId.of("B"));
        Assert.assertFalse(taskHolder.onLogCommitted(entry));
    }

    @Test
    public void testOnLogCommittedAddNodeTask() throws InterruptedException, ExecutionException {
        WaitableGroupConfigChangeTaskContext taskContext = new WaitableGroupConfigChangeTaskContext();
        AddNodeTask task = new AddNodeTask(
                taskContext,
                new NodeEndpoint("D", "localhost", 2336),
                2, 1
        );
        GroupConfigChangeTaskHolder taskHolder = new GroupConfigChangeTaskHolder(
                task, new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK)
        );
        Future<GroupConfigChangeTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitLogAppended();
        AddNodeEntry entry = new AddNodeEntry(
                1, 1, Collections.emptySet(), new NodeEndpoint("D", "localhost", 2336)
        );
        Assert.assertTrue(taskHolder.onLogCommitted(entry));
        future.get();
    }

    // not target node
    @Test
    public void testOnLogCommittedAddNodeTask2() {
        AddNodeTask task = new AddNodeTask(
                new WaitableGroupConfigChangeTaskContext(),
                new NodeEndpoint("D", "localhost", 2336),
                2, 1
        );
        GroupConfigChangeTaskHolder taskHolder = new GroupConfigChangeTaskHolder(
                task, new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK)
        );
        AddNodeEntry entry = new AddNodeEntry(
                1, 1, Collections.emptySet(), new NodeEndpoint("E", "localhost", 2337)
        );
        Assert.assertFalse(taskHolder.onLogCommitted(entry));
    }

    @Test
    public void testOnLogCommittedRemoveNodeTask() throws ExecutionException, InterruptedException {
        WaitableGroupConfigChangeTaskContext taskContext = new WaitableGroupConfigChangeTaskContext();
        RemoveNodeTask task = new RemoveNodeTask(taskContext, NodeId.of("B"));
        Future<GroupConfigChangeTaskResult> future = taskExecutor.submit(task);
        GroupConfigChangeTaskHolder taskHolder = new GroupConfigChangeTaskHolder(
                task, new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK)
        );
        taskContext.awaitLogAppended();
        RemoveNodeEntry entry = new RemoveNodeEntry(1, 1, Collections.emptySet(), NodeId.of("B"));
        Assert.assertTrue(taskHolder.onLogCommitted(entry));
        future.get();
    }

    // not target node
    @Test
    public void testOnLogCommittedRemoveNodeTask2() throws ExecutionException, InterruptedException {
        WaitableGroupConfigChangeTaskContext taskContext = new WaitableGroupConfigChangeTaskContext();
        RemoveNodeTask task = new RemoveNodeTask(taskContext, NodeId.of("B"));
        GroupConfigChangeTaskHolder taskHolder = new GroupConfigChangeTaskHolder(
                task, new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK)
        );
        RemoveNodeEntry entry = new RemoveNodeEntry(1, 1, Collections.emptySet(), NodeId.of("C"));
        Assert.assertFalse(taskHolder.onLogCommitted(entry));
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        taskExecutor.shutdown();
    }

}