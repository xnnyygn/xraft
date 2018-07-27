package in.xnnyygn.xraft.core.service;

public class NullStateMachine implements StateMachine {

    @Override
    public void applyCommand(byte[] commandBytes) {
    }

    @Override
    public void applySnapshot(byte[] snapshot) {
    }

    @Override
    public byte[] generateSnapshot() {
        return new byte[0];
    }

}
