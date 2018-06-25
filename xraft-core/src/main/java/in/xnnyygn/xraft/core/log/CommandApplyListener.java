package in.xnnyygn.xraft.core.log;

public interface CommandApplyListener {

    void applyCommand(int index, byte[] command);

}
