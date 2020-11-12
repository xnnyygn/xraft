package in.xnnyygn.xraft.core.stage;

public interface MessageListener<T> {
    boolean definedAt(T message);
    void apply(T message);
}
