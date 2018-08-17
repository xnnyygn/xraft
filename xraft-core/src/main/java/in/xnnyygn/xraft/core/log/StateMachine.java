package in.xnnyygn.xraft.core.log;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * State machine.
 */
public interface StateMachine {

    /**
     * Apply log.
     *
     * @param index log index
     * @param commandBytes command bytes
     */
    void applyLog(int index, @Nonnull byte[] commandBytes);

    /**
     * Generate snapshot to output.
     *
     * @param output output
     * @throws IOException if IO error occurred
     */
    void generateSnapshot(@Nonnull OutputStream output) throws IOException;

    /**
     * Apply snapshot.
     *
     * @param input input
     * @throws IOException if IO error occurred
     */
    void applySnapshot(@Nonnull InputStream input) throws IOException;

}
