package in.xnnyygn.xraft.core.stage;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AbstractRpcMessage;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NewNodeCatchUpStage {
    private static final Logger logger = LoggerFactory.getLogger(NewNodeCatchUpStage.class);

    private final StageContext context;
    private final CatchUpRuntime runtime;
    private final ConcurrentMap<NodeId, CatchUpTask> taskMap = new ConcurrentHashMap<>();

    public NewNodeCatchUpStage(StageContext context, int threadCount) {
        this.context = context;
        runtime = new CatchUpRuntime(threadCount);
    }

    public void start() {
        runtime.start();
    }

    public void receive(@Nonnull NewNodeMessage newNodeMessage) {
        int threadId = runtime.nextThreadId();
        CatchUpTask task = new CatchUpTask(context, threadId, newNodeMessage);
        if (taskMap.putIfAbsent(newNodeMessage.getNodeId(), task) != null) {
            // task for node exists
            newNodeMessage.reply(NewNodeMessage.STATUS_NODE_DUPLICATED);
            return;
        }
        CatchUpMessageListener messageListener = new CatchUpMessageListener(runtime, task);
        context.addMessageListener(messageListener);
        task.onDone = () -> context.removeMessageListener(messageListener);
        // submit task
        runtime.submit(task::start, task.threadId);
    }

    public void stop() throws InterruptedException {
        runtime.stop();
    }

    // single thread
    // bind to one thread
    // thread id interface
    // one task one time
    private static class CatchUpRuntime2 {
        private static final int MAX_THREADS = 3;
        private final Thread[] threads = new Thread[MAX_THREADS];
        private final AtomicInteger threadCount = new AtomicInteger(0);

        private final CatchUpRuntimeWorker[] workers = new CatchUpRuntimeWorker[MAX_THREADS];



        public void submit(CatchUpTask task) {
            // add new task
            // threads 0 ~ n
            // try to add to queue?
            // start new thread
            // thread idle
            // thread queue, take timeout

            // queue: T1, T2, T3

            // maximum, create new worker every time

            // create threads
//            int c;
//            while (true) {
//                c = threadCount.get();
//                if (c < MAX_THREADS) {
//                    if (threadCount.compareAndSet(c, c + 1)) {
//                        // thread c can be created
//                    } else {
//                        // retry
//                    }
//                } else {
//                    // no more thread can be created
//                }
//            }

            // created thread put into pool
            // single write, multiple read?

            // reasonable thread count
            // workers ring
            // create ring when initialize
            // W1, W2, W3

            // thread pool model
            // cannot specify thread id

            // submit and return thread id
            // if thread count not reach max
            //   create new thread and handle task by new thread
            // else
            //   add task to queue

            // thread exit
            // no task + timeout

        }

    }

    private static class CatchUpRuntimeWorker {

    }

    // single thread one time
    private static class CatchUpRuntime {
        final Random random = new Random();
        final int threadCount;
        final BlockingQueue<Runnable>[] queues;
        final Thread[] workers;
        final CountDownLatch stopLatch;

        @SuppressWarnings("unchecked")
        CatchUpRuntime(int threadCount) {
            if (threadCount <= 0) {
                throw new IllegalArgumentException("thread count <= 0");
            }
            this.threadCount = threadCount;
            queues = (BlockingQueue<Runnable>[]) new BlockingQueue[threadCount];
            workers = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                int threadId = i;
                queues[threadId] = new LinkedBlockingQueue<>();
                workers[threadId] = new Thread(() -> worker(threadId), "catchup-runtime-" + threadId);
            }
            stopLatch = new CountDownLatch(threadCount);
        }

        void start() {
            for (Thread w : workers) {
                w.start();
            }
        }

        void stop() throws InterruptedException {
            for (Thread w : workers) {
                w.interrupt();
            }
            stopLatch.await();
        }

        int nextThreadId() {
            return random.nextInt(threadCount);
        }

        void submit(Runnable task, int threadId) {
            if (threadId < 0 || threadId >= threadCount) {
                throw new IllegalArgumentException("illegal thread id " + threadId);
            }
            queues[threadId].offer(task);
        }

        private void worker(int threadId) {
            try {
                while (true) {
                    Runnable task = queues[threadId].take();
                    runTask(task);
                }
            } catch (InterruptedException ignored) {
            }
            stopLatch.countDown();
        }

        private void runTask(Runnable task) {
            try {
                task.run();
            } catch (RuntimeException ignored) {
            }
        }
    }

    private static class CatchUpMessageListener implements MessageListener<AbstractRpcMessage<?>> {
        final CatchUpRuntime runtime;
        final CatchUpTask task;

        CatchUpMessageListener(CatchUpRuntime runtime, CatchUpTask task) {
            this.runtime = runtime;
            this.task = task;
        }

        @Override
        public boolean definedAt(AbstractRpcMessage<?> message) {
            return task.getNodeId().equals(message.getSourceNodeId());
        }

        @Override
        public void apply(AbstractRpcMessage<?> message) {
            runtime.submit(() -> task.onReceive(message), task.threadId);
        }
    }

    private static class CatchUpTask {
        static final int STATUS_CODE_INITIAL = -1;

        final StageContext context;
        final int threadId;
        final NewNodeMessage newNodeMessage;
        final AtomicInteger statusCode = new AtomicInteger(STATUS_CODE_INITIAL);
        TimeoutTimer timeoutTimer = null;
        volatile Runnable onDone = null;
        int nextIndex;

        CatchUpTask(StageContext context, int threadId, NewNodeMessage newNodeMessage) {
            this.context = context;
            this.threadId = threadId;
            this.newNodeMessage = newNodeMessage;
        }

        NodeId getNodeId() {
            return newNodeMessage.getNodeId();
        }

        void start() {
            timeoutTimer = context.scheduleTimeout(this::onTimeout, 3, TimeUnit.SECONDS);
            timeoutTimer.start();
            // TODO send first message
        }

        void onReceive(AbstractRpcMessage<?> rpcMessage) {
            if (statusCode.get() != STATUS_CODE_INITIAL) {
                // timeout
                return;
            }
            timeoutTimer.cancel();
            // TODO process message
            Object message = rpcMessage.get();

            timeoutTimer.start();
        }

        // TODO add next log index in result message
        private void onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
            AppendEntriesResult result = resultMessage.get();
            if (result.isSuccess()) {

            }
        }

        void onTimeout() {
            if (statusCode.get() == STATUS_CODE_INITIAL &&
                    statusCode.compareAndSet(STATUS_CODE_INITIAL, NewNodeMessage.STATUS_TIMEOUT)) {
                newNodeMessage.reply(NewNodeMessage.STATUS_TIMEOUT);
                done();
            }
        }

        private void done() {
            if (onDone != null) {
                onDone.run();
            }
        }
    }
}
