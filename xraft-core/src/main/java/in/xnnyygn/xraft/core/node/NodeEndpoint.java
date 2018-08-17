package in.xnnyygn.xraft.core.node;

import com.google.common.base.Preconditions;
import in.xnnyygn.xraft.core.rpc.Address;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.Objects;

/**
 * Node endpoint.
 */
@Immutable
public class NodeEndpoint {

    private final NodeId id;
    private final Address address;

    /**
     * Create.
     *
     * @param id node id
     * @param host host
     * @param port port
     */
    public NodeEndpoint(@Nonnull String id, @Nonnull String host, int port) {
        this(new NodeId(id), new Address(host, port));
    }

    /**
     * Create.
     *
     * @param id id
     * @param address address
     */
    public NodeEndpoint(@Nonnull NodeId id, @Nonnull Address address) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(address);
        this.id = id;
        this.address = address;
    }

    /**
     * Get id.
     *
     * @return id
     */
    @Nonnull
    public NodeId getId() {
        return this.id;
    }

    /**
     * Get host.
     *
     * @return host
     */
    @Nonnull
    public String getHost() {
        return this.address.getHost();
    }

    /**
     * Get port.
     *
     * @return port
     */
    public int getPort() {
        return this.address.getPort();
    }

    /**
     * Get address.
     *
     * @return address
     */
    @Nonnull
    public Address getAddress() {
        return this.address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeEndpoint)) return false;
        NodeEndpoint that = (NodeEndpoint) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "NodeEndpoint{id=" + id + ", address=" + address + '}';
    }

}
