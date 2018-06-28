package in.xnnyygn.xraft.core.rpc.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SocketChannelExecutorServicePool {

    private static final Logger logger = LoggerFactory.getLogger(SocketChannelExecutorServicePool.class);
    private final Set<ExecutorService> using = new HashSet<>();
    private final Set<ExecutorService> available = new HashSet<>();
    private int counter = 1;

    public ExecutorService borrowExecutorService() {
        logger.debug("borrow executor");
        if (this.available.isEmpty()) {
            logger.debug("create executor {}", counter);
            ExecutorService executorService = Executors.newSingleThreadExecutor(
                    r -> new Thread(r, "socket-channel-executor-" + (counter++))
            );
            this.using.add(executorService);
            return executorService;
        }

        ExecutorService executorService = this.available.iterator().next();
        this.available.remove(executorService);
        this.using.add(executorService);
        return executorService;
    }

    public void returnExecutorService(ExecutorService executorService) {
        logger.debug("return executor");
        this.using.remove(executorService);
        this.available.add(executorService);
    }

    public void release() {
        this.release(this.using);
        this.release(this.available);
    }

    private void release(Collection<ExecutorService> executorServices) {
        for (ExecutorService executorService : executorServices) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(1L, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

}
