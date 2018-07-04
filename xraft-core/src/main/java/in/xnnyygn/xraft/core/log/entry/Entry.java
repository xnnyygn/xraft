package in.xnnyygn.xraft.core.log.entry;

public interface Entry {

    int getIndex();

    int getTerm();

    byte[] getCommandBytes();

}
