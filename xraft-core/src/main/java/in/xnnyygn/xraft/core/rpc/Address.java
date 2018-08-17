package in.xnnyygn.xraft.core.rpc;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Address {

    private final String host;
    private final int port;

    public Address(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "Address{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

}
