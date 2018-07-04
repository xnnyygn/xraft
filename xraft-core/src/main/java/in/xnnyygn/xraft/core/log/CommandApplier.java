package in.xnnyygn.xraft.core.log;

public interface CommandApplier {

    void applyCommand(byte[] commandBytes);

}
