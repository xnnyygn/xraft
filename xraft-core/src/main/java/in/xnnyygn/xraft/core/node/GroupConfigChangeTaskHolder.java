package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.entry.AddNodeEntry;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.entry.RemoveNodeEntry;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class GroupConfigChangeTaskHolder {

    private static final Logger logger = LoggerFactory.getLogger(GroupConfigChangeTaskHolder.class);
    private final GroupConfigChangeTask task;
    private final GroupConfigChangeTaskReference reference;

    GroupConfigChangeTaskHolder() {
        this(GroupConfigChangeTask.NONE, new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK));
    }

    GroupConfigChangeTaskHolder(GroupConfigChangeTask task, GroupConfigChangeTaskReference reference) {
        this.task = task;
        this.reference = reference;
    }

    void await(long timeout) throws TimeoutException, InterruptedException {
        reference.getResult(timeout);
    }

    void cancel() {
        reference.cancel();
    }

    public boolean isEmpty() {
        return task == GroupConfigChangeTask.NONE;
    }

    boolean onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage, int nextLogIndex) {
        if(isEmpty()) {
            return false;
        }
        logger.debug("receive append entries result, current task {}", task);
        if(task.isTargetNode(resultMessage.getSourceNodeId()) && task instanceof AddNodeTask) {
            ((AddNodeTask) task).onReceiveAppendEntriesResult(resultMessage, nextLogIndex);
            return true;
        }
        return false;
    }

    boolean onLogCommitted(GroupConfigEntry entry) {
        if(isEmpty()) {
            return false;
        }
        logger.debug("log committed, current task {}", task);
        if(entry instanceof AddNodeEntry && task instanceof AddNodeTask
                && task.isTargetNode(((AddNodeEntry) entry).getNewNodeEndpoint().getId())) {
            task.onLogCommitted();
            return true;
        }
        if(entry instanceof RemoveNodeEntry && task instanceof RemoveNodeTask
                && task.isTargetNode(((RemoveNodeEntry) entry).getNodeToRemove())) {
            task.onLogCommitted();
            return true;
        }
        return false;
    }

}
