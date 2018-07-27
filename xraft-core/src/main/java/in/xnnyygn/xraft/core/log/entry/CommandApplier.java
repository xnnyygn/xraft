package in.xnnyygn.xraft.core.log.entry;

public interface CommandApplier {

    void applyCommand(byte[] commandBytes);

}
