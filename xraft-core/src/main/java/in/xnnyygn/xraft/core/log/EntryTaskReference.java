package in.xnnyygn.xraft.core.log;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EntryTaskReference implements TaskReference {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile Result result;
    private int entryIndex;

    public void done(Result result) {
        this.result = result;
        latch.countDown();
    }

    public int getEntryIndex() {
        return entryIndex;
    }

    public void setEntryIndex(int entryIndex) {
        this.entryIndex = entryIndex;
    }

    @Override
    public Result getResult() {
        return result;
    }

    @Override
    public boolean await(long timeout) throws InterruptedException {
        return latch.await(timeout, TimeUnit.MILLISECONDS);
    }

}
