package in.xnnyygn.xraft.core.log.entry;

import java.util.List;

public interface EntrySequence {

    boolean isEmpty();

    int getFirstLogIndex();

    int getLastLogIndex();

    int getNextLogIndex();

    int getAndIncreaseNextLogIndex();

    List<Entry> subList(int fromIndex);

    // [fromIndex, toIndex)
    List<Entry> subList(int fromIndex, int toIndex);

    Entry getEntry(int index);

    Entry getLastEntry();

    void append(Entry entry);

    void append(List<Entry> entries);

    void commit(int index);

    void removeAfter(int index);

    void close();

}
