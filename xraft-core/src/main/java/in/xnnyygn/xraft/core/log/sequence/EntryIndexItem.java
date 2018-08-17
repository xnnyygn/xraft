package in.xnnyygn.xraft.core.log.sequence;

import in.xnnyygn.xraft.core.log.entry.EntryMeta;

import javax.annotation.concurrent.Immutable;

@Immutable
class EntryIndexItem {

    private final int index;
    private final long offset;
    private final int kind;
    private final int term;

    EntryIndexItem(int index, long offset, int kind, int term) {
        this.index = index;
        this.offset = offset;
        this.kind = kind;
        this.term = term;
    }

    int getIndex() {
        return index;
    }

    long getOffset() {
        return offset;
    }

    int getKind() {
        return kind;
    }

    int getTerm() {
        return term;
    }

    EntryMeta toEntryMeta() {
        return new EntryMeta(kind, index, term);
    }

}
