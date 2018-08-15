package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.rpc.Address;

import java.util.Objects;

public class NodeEndpoint {

    private final NodeId id;
    private final Address address;

    public NodeEndpoint(String id, String host, int port) {
        this(new NodeId(id), new Address(host, port));
    }

    public NodeEndpoint(NodeId id, Address address) {
        this.id = id;
        this.address = address;
    }

    public NodeId getId() {
        return this.id;
    }

    public String getHost() {
        return this.address.getHost();
    }

    public int getPort() {
        return this.address.getPort();
    }

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
