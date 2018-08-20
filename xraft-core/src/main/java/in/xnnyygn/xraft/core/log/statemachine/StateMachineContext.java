package in.xnnyygn.xraft.core.log.statemachine;

public interface StateMachineContext {

    void generateSnapshot(int lastIncludedIndex);

}
