package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.node.NodeEndpoint;

import java.io.OutputStream;
import java.util.Set;

public interface StateMachineContext {

    @Deprecated
    void generateSnapshot(int lastIncludedIndex);

    OutputStream getOutputForGeneratingSnapshot(int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> groupConfig) throws Exception;

    void doneGeneratingSnapshot(int lastIncludedIndex) throws Exception;

}
