package in.xnnyygn.xraft.core.log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NullStateMachine implements StateMachine {

    @Override
    public void applyLog(int index, byte[] commandBytes) {
    }

    @Override
    public void generateSnapshot(OutputStream output) throws IOException {
    }

    @Override
    public void applySnapshot(InputStream input) throws IOException {
    }

}
