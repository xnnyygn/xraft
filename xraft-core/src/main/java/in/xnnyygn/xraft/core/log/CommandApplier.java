package in.xnnyygn.xraft.core.log;

public interface CommandApplier {

    void applyCommand(int index, byte[] command);

}
