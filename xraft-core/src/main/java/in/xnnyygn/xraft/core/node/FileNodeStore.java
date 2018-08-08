package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.support.Files;
import in.xnnyygn.xraft.core.support.RandomAccessFileAdapter;
import in.xnnyygn.xraft.core.support.SeekableFile;

import java.io.File;
import java.io.IOException;

public class FileNodeStore implements NodeStore {

    public static final String FILE_NAME = "node.bin";
    private static final long OFFSET_CURRENT_TERM = 0;
    private static final long OFFSET_VOTED_FOR = 4;
    private final SeekableFile seekableFile;

    public FileNodeStore(File file) {
        try {
            if (!file.exists()) {
                Files.touch(file);
            }
            seekableFile = new RandomAccessFileAdapter(file);
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

    public FileNodeStore(SeekableFile seekableFile) {
        this.seekableFile = seekableFile;
    }

    public void initialize() {
        try {
            if (seekableFile.size() > 0) {
                return;
            }

            // (term, 4) + (votedFor length, 4) = 8
            seekableFile.truncate(8L);
            seekableFile.seek(0);
            seekableFile.writeInt(0); // term
            seekableFile.writeInt(0); // votedFor length
            seekableFile.seek(0); // return to start
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

    @Override
    public int getCurrentTerm() {
        try {
            seekableFile.seek(OFFSET_CURRENT_TERM);
            return seekableFile.readInt();
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

    @Override
    public void setCurrentTerm(int currentTerm) {
        try {
            seekableFile.seek(OFFSET_CURRENT_TERM);
            seekableFile.writeInt(currentTerm);
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

    @Override
    public NodeId getVotedFor() {
        try {
            seekableFile.seek(OFFSET_VOTED_FOR);
            int length = seekableFile.readInt();
            if (length == 0) {
                return null;
            }
            byte[] bytes = new byte[length];
            seekableFile.read(bytes);
            return new NodeId(new String(bytes));
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
    }

    @Override
    public void setVotedFor(NodeId votedFor) {
        try {
            seekableFile.seek(OFFSET_VOTED_FOR);
            if (votedFor == null) {
                seekableFile.writeInt(0);
            } else {
                byte[] bytes = votedFor.getValue().getBytes();
                seekableFile.writeInt(bytes.length);
                seekableFile.write(bytes);
            }
        } catch (IOException e) {
            throw new NodeStoreException(e);
        }
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
