package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.GeneralEntry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

class EntryApplyRecorder implements StateMachine {

    private final List<Entry> entries = new ArrayList<>();

    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public void applyLog(int index, byte[] commandBytes) {
        entries.add(new GeneralEntry(index, -1, commandBytes));
    }

    @Override
    public void generateSnapshot(OutputStream output) throws IOException {

    }

    @Override
    public void applySnapshot(InputStream input) throws IOException {

    }
}
