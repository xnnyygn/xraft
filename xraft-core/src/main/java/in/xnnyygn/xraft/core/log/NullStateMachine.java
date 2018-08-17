package in.xnnyygn.xraft.core.log;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NullStateMachine implements StateMachine {

    @Override
    public void applyLog(int index, @Nonnull byte[] commandBytes) {
    }

    @Override
    public void generateSnapshot(@Nonnull OutputStream output) throws IOException {
    }

    @Override
    public void applySnapshot(@Nonnull InputStream input) throws IOException {
    }

}
