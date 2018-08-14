package in.xnnyygn.xraft.core.log.entry;

public interface Entry {

    int KIND_NO_OP = 0;
    int KIND_GENERAL = 1;
    int KIND_ADD_NODE = 3;
    int KIND_REMOVE_NODE = 4;

    int getKind();

    int getIndex();

    int getTerm();

    EntryMeta getMeta();

    byte[] getCommandBytes();

}
