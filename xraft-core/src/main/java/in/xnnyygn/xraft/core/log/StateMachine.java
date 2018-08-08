package in.xnnyygn.xraft.core.log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StateMachine {

    void applyLog(int index, byte[] commandBytes);

    void generateSnapshot(OutputStream output) throws IOException;

    void applySnapshot(InputStream input) throws IOException;

}
