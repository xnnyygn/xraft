package in.xnnyygn.xraft.core.log;

public class MemorySnapshot {

    private final byte[] data;
    private int position = 0;

    public MemorySnapshot(byte[] data) {
        this.data = data;
    }

    public int size() {
        return this.data.length;
    }

    public void seek(int position) {
        if (position < 0 || position >= this.data.length) {
            throw new IndexOutOfBoundsException("position out of index");
        }
        this.position = position;
    }

    public int read(byte[] buffer) {
        if (this.position >= this.data.length) return -1;

        int length = Math.min(buffer.length, this.data.length - this.position);
        System.arraycopy(this.data, this.position, buffer, 0, length);
        this.position += length;
        return length;
    }

    public void close() {
    }

}
