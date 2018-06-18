package in.xnnyygn.xraft.server;

import java.io.Serializable;
import java.util.Objects;

public class ServerId implements Serializable {

    private final String value;

    public ServerId(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServerId)) return false;
        ServerId id = (ServerId) o;
        return Objects.equals(value, id.value);
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
