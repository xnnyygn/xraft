package in.xnnyygn.xraft.core.node;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Objects;

/**
 * Node id.
 */
@Immutable
public class NodeId implements Serializable {

    private final String value;

    /**
     * Create.
     *
     * @param value value
     */
    public NodeId(@Nonnull String value) {
        Preconditions.checkNotNull(value);
        this.value = value;
    }

    /**
     * Create.
     *
     * @param value value
     * @return node id
     */
    public static NodeId of(@Nonnull String value) {
        return new NodeId(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeId)) return false;
        NodeId id = (NodeId) o;
        return Objects.equals(value, id.value);
    }

    /**
     * Get value.
     *
     * @return value
     */
    @Nonnull
    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return this.value;
    }

}
