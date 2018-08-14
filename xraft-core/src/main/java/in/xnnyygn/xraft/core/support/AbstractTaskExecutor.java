package in.xnnyygn.xraft.core.support;

import com.google.common.util.concurrent.FutureCallback;

import java.util.Collections;

public abstract class AbstractTaskExecutor implements TaskExecutor {

    @Override
    public void submit(Runnable task, FutureCallback<Object> callback) {
        submit(task, Collections.singletonList(callback));
    }

}
