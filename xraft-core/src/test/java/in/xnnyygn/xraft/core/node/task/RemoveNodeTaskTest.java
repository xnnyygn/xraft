package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.support.ListeningTaskExecutor;
import in.xnnyygn.xraft.core.support.TaskExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RemoveNodeTaskTest {

    private static TaskExecutor taskExecutor;

    @BeforeClass
    public static void beforeClass() {
        taskExecutor = new ListeningTaskExecutor(Executors.newSingleThreadExecutor());
    }

    @Test
    public void testNormal() throws InterruptedException, ExecutionException {
        WaitableGroupConfigChangeTaskContext taskContext = new WaitableGroupConfigChangeTaskContext();
        RemoveNodeTask task = new RemoveNodeTask(
                taskContext,
                NodeId.of("D"),
                NodeId.of("A"));
        Future<GroupConfigChangeTaskResult> future = taskExecutor.submit(task);
        taskContext.awaitLogAppended();
        task.onLogCommitted(null);
        Assert.assertEquals(GroupConfigChangeTaskResult.OK, future.get());
    }

    @Test(expected = IllegalStateException.class)
    public void testOnLogCommittedLogNotAppended() {
        RemoveNodeTask task = new RemoveNodeTask(
                new WaitableGroupConfigChangeTaskContext(),
                NodeId.of("D"),
                NodeId.of("A"));
        task.onLogCommitted(null);
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        taskExecutor.shutdown();
    }

}