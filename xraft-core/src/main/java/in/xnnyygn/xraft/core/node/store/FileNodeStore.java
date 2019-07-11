package in.xnnyygn.xraft.core.node.store;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.support.Files;
import in.xnnyygn.xraft.core.support.RandomAccessFileAdapter;
import in.xnnyygn.xraft.core.support.SeekableFile;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;

@NotThreadSafe
public class FileNodeStore implements NodeStore {

    public static final String FILE_NAME = "node.bin";
    private static final long OFFSET_TERM = 0;
    private static final long OFFSET_VOTED_FOR = 4;
    private final SeekableFile seekableFile;
    private int term = 0;
    private NodeId votedFor = null;

    public FileNodeStore(File file) {
        try {
            if (!file.exists()) {
                Files.touch(file);
            }
            seekableFile = new RandomAccessFileAdapter(file);
            initializeOrLoad();
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

    public FileNodeStore(SeekableFile seekableFile) {
        this.seekableFile = seekableFile;
        try {
            initializeOrLoad();
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

    private void initializeOrLoad() throws IOException {
        if (seekableFile.size() == 0) {
            // (term, 4) + (votedFor length, 4) = 8
            seekableFile.truncate(8L);
            seekableFile.seek(0);
            seekableFile.writeInt(0); // term
            seekableFile.writeInt(0); // votedFor length
        } else {
            // read term
            term = seekableFile.readInt();
            // read voted for
            int length = seekableFile.readInt();
            if (length > 0) {
                byte[] bytes = new byte[length];
                seekableFile.read(bytes);
                votedFor = new NodeId(new String(bytes));
            }
        }
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public void setTerm(int term) {
        try {
            seekableFile.seek(OFFSET_TERM);
            seekableFile.writeInt(term);
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
        this.term = term;
    }

    @Override
    public NodeId getVotedFor() {
        return votedFor;
    }

    @Override
    public void setVotedFor(NodeId votedFor) {
        try {
            seekableFile.seek(OFFSET_VOTED_FOR);
            if (votedFor == null) {
                seekableFile.writeInt(0);
                seekableFile.truncate(8L);
            } else {
                byte[] bytes = votedFor.getValue().getBytes();
                seekableFile.writeInt(bytes.length);
                seekableFile.write(bytes);
            }
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
        this.votedFor = votedFor;
    }

    @Override
    public void close() {
        try {
            seekableFile.close();
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

}
