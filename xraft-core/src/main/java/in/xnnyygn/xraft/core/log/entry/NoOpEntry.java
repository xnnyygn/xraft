package in.xnnyygn.xraft.core.log.entry;

public class NoOpEntry extends AbstractEntry {

    public NoOpEntry(int index, int term) {
        super(index, term);
    }

    @Override
    public byte[] getCommandBytes() {
        return new byte[0];
    }

    @Override
    public String toString() {
        return "NoOpEntry{" +
                "index=" + index +
                ", term=" + term +
                '}';
    }

}
