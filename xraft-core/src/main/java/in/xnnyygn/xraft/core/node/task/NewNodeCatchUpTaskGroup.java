package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResultMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NewNodeCatchUpTaskGroup {

    private final ConcurrentMap<NodeId, NewNodeCatchUpTask> taskMap = new ConcurrentHashMap<>();

    public boolean add(NewNodeCatchUpTask task) {
        return taskMap.putIfAbsent(task.getNodeId(), task) == null;
    }

    public boolean onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage, int nextLogIndex) {
        NewNodeCatchUpTask task = taskMap.get(resultMessage.getSourceNodeId());
        if(task == null) {
            return false;
        }
        task.onReceiveAppendEntriesResult(resultMessage, nextLogIndex);
        return true;
    }

    public void remove(NewNodeCatchUpTask task) {
        taskMap.remove(task.getNodeId());
    }

}
