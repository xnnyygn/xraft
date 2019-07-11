package in.xnnyygn.xraft.core.log.sequence;

import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.node.NodeEndpoint;

import java.util.List;
import java.util.Set;

public interface EntrySequence {

    boolean isEmpty();

    int getFirstLogIndex();

    int getLastLogIndex();

    int getNextLogIndex();

    List<Entry> subView(int fromIndex);

    // [fromIndex, toIndex)
    List<Entry> subList(int fromIndex, int toIndex);

    GroupConfigEntryList buildGroupConfigEntryList(Set<NodeEndpoint> initialGroup);

    boolean isEntryPresent(int index);

    EntryMeta getEntryMeta(int index);

    Entry getEntry(int index);

    Entry getLastEntry();

    void append(Entry entry);

    void append(List<Entry> entries);

    void commit(int index);

    int getCommitIndex();

    void removeAfter(int index);

    void close();

}
