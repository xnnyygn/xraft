package in.xnnyygn.xraft.core.node.config;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;

public interface NodeConfigLoader {

    @Nonnull
    NodeConfig load(@Nonnull InputStream input) throws IOException;

}
