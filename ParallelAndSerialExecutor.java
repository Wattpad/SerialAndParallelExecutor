package wp.wattpad.util.threading;

import wp.wattpad.internal.model.stories.Story;
import wp.wattpad.util.logger.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The ParallelAndSerialExecutor is an executor which will run tasks in parallel or serially depending on what tasks are passed in.
 *
 * To execute tasks serially, call {@link #execute(Runnable)} with a runnable that extends {@link SerialRunnable}.
 * The executor will guarantee execution of any runnables with the same identifier in a serial order.
 *
 * All other tasks which do not implement the {@link SerialRunnable} will be executed in a parallel fashion.
 *
 * @param <T> The type of object the serial executor will use to identify equality (via .equals() and hashcode()). All equal objects will be guaranteed to be executed serially.
 * @author ianko
 */
public class ParallelAndSerialExecutor<T> {

    /**
     * The interface to implement for any {@link java.lang.Runnable} task to be executed in serial order.
     */
    public interface SerialRunnable<T> extends Runnable {
        /**
         * The objects for the runnables to be executed serially
         */
        public T getIdentifier();
    }

    /**
     * The underlying executor that is responsible for executing runnables in a parallel or serial fashion.
     * We wrap the underlying implementation because we only want to expose a handful of methods for use.
     */
    private ParallelAndSerialExecutorImpl parallelAndSerialExecutor;

    /**
     * The possible states for the executor
     */
    private enum State {
        RUNNING, SHUTDOWN
    }

    /**
     * Creates a new {@code ParallelAndSerialExecutor} with the given initial
     * parameters.
     *
     * @param numCoreThreads the number of threads to keep in the pool, even
     * if they are idle
     * @param maxNumThreads the maximum number of threads to allow in the
     * pool
     * @param threadKeepAliveSec when the number of threads is greater than
     * the core, this is the maximum time that excess idle threads
     * will wait for new tasks before terminating.
     */
    public ParallelAndSerialExecutor(int numCoreThreads, int maxNumThreads, int threadKeepAliveSec) {
        parallelAndSerialExecutor = new ParallelAndSerialExecutorImpl<T>(numCoreThreads, maxNumThreads, threadKeepAliveSec);
    }

    /**
     * Executes the runnable.  If the runnable implements {@link SerialRunnable}, it will be executed with a SerialExecutor. All others will be executed in a parallel fashion.
     */
    public void execute(Runnable runnable) {
        if (runnable != null) {
            parallelAndSerialExecutor.execute(runnable);
        }
    }

    /**
     * The internal implementation of the parallel and serial executor.
     * We wrap the underlying implementation because we only want to expose a handful of methods for use.
     */
    private class ParallelAndSerialExecutorImpl<T> extends AbstractExecutorService {
        /**
         * The executor responsible for executing all tasks
         */
        private final ExecutorService executor;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition terminating = lock.newCondition();
        private State state = State.RUNNING;

        /**
         * Whenever a new {@link SerialRunnable} is submitted to the pool, it is added to the map with a corresponding
         * {@link SerialExecutor} object. As soon as the SerialExecutor is empty, the entry is removed from the map
         */
        private final Map<T, SerialExecutor> serialExecutorMap = new HashMap<>();

        /**
         * Constructs an ParallelAndSerialExecutorImpl
         */
        protected ParallelAndSerialExecutorImpl(int numCoreThreads, int maxNumThreads, int threadKeepAliveSec) {
            executor = new ThreadPoolExecutor(numCoreThreads, maxNumThreads, threadKeepAliveSec, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        }

        /**
         * Executes the runnable.  If the runnable implements {@link SerialRunnable}, it will be executed with a SerialExecutor.
         * All others will be executed in a parallel fashion.
         */
        public void execute(Runnable runnable) {
            lock.lock();
            try {
                if (state != State.RUNNING || !lock.isHeldByCurrentThread()) {
                    throw new RejectedExecutionException("executor not running");
                }
                T identifier = getIdentifier(runnable);
                if (identifier != null) {
                    //We need to execute this runnable serially
                    SerialExecutor serialExecutor = serialExecutorMap.get(identifier);
                    if (serialExecutor == null) {
                        serialExecutor = new SerialExecutor<T>(identifier);
                        serialExecutorMap.put(identifier, serialExecutor);
                    }
                    serialExecutor.execute(runnable);
                } else {
                    executor.execute(runnable);
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Returns the identifier from the serial runnable if one exists, or null if none found
         */
        private T getIdentifier(Runnable runnable) {
            T id = null;
            if (runnable instanceof SerialRunnable) {
                //Grab the identifier
                id = (((SerialRunnable<T>) runnable).getIdentifier());
            }
            return id;
        }

        /**
         * Shuts down the executor.  No more tasks will be run.  If the map of SerialExecutors is empty, we shut down the wrapped executor.
         */
        public void shutdown() {
            lock.lock();
            try {
                state = State.SHUTDOWN;
                if (serialExecutorMap.isEmpty()) {
                    executor.shutdown();
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * All the tasks in each of the SerialExecutors are drained to a list, as well as the tasks inside the wrapped ExecutorService.
         * This is then returned to the user.  Also, the shutdownNow method of the wrapped executor is called.
         */
        public List<Runnable> shutdownNow() {
            lock.lock();
            try {
                shutdown();
                List<Runnable> result = new ArrayList<>();
                for (SerialExecutor serialExecutor : serialExecutorMap.values()) {
                    serialExecutor.tasks.drainTo(result);
                }
                result.addAll(executor.shutdownNow());
                return result;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Returns true if shutdown() or shutdownNow() have been called. false otherwise.
         */
        public boolean isShutdown() {
            lock.lock();
            try {
                return state == State.SHUTDOWN;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Returns true if this pool has been terminated, that is, all the SerialExecutors are empty and the wrapped ExecutorService has been terminated.
         */
        public boolean isTerminated() {
            lock.lock();
            try {
                if (state == State.RUNNING) {
                    return false;
                }

                for (SerialExecutor executor : serialExecutorMap.values()) {
                    if (!executor.isEmpty()) {
                        return false;
                    }
                }
                return executor.isTerminated();
            } finally {
                lock.unlock();
            }
        }

        /**
         * Returns true if the wrapped ExecutorService terminates within the allotted amount of time.
         */
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            lock.lock();
            try {
                long waitUntil = System.nanoTime() + unit.toNanos(timeout);
                long remainingTime = waitUntil - System.nanoTime();
                while (remainingTime > 0 && !serialExecutorMap.isEmpty()) {
                    terminating.awaitNanos(remainingTime);
                    remainingTime = waitUntil - System.nanoTime();
                }

                if (remainingTime <= 0) {
                    return false;
                }

                if (serialExecutorMap.isEmpty()) {
                    return executor.awaitTermination(remainingTime, TimeUnit.NANOSECONDS);
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        /**
         * SerialExecutor is an executor which holds all runnables with a specific identifier. Tasks are executed in the order they are inserted serially.
         * It will continue to execute tasks until it has no more, and then terminate and remove itself from the {@link #serialExecutorMap}
         */
        private class SerialExecutor<T> implements Executor {

            private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
            private Runnable active;

            /**
             * The unique id that this SerialExecutor was defined for
             */
            private final T identifier;

            /**
             * Creates a SerialExecutor for a particular identifier.
             */
            private SerialExecutor(T identifier) {
                this.identifier = identifier;
            }

            @Override
            protected void finalize() throws Throwable {
                //Do nothing
            }

            /**
             * For every task that is executed, we attempt to schedule the next job when it is done to ensure everything is run serially
             */
            public void execute(final Runnable r) {
                lock.lock();
                try {
                    tasks.add(new Runnable() {
                        public void run() {
                            try {
                                r.run();
                            } finally {
                                scheduleNext();
                            }
                        }
                    });
                    if (active == null) {
                        scheduleNext();
                    }
                } finally {
                    lock.unlock();
                }
            }

            /**
             * Schedules the next task for this serial executor.  Should only be called if active == null or if we are finished executing the currently active task.
             */
            private void scheduleNext() {
                lock.lock();
                try {
                    active = tasks.poll();
                    if (active != null) {
                        executor.execute(active);
                        terminating.signalAll();
                    } else {
                        //As soon as a SerialExecutor is empty, we remove it from the executors map.
                        if (lock.isHeldByCurrentThread() && isEmpty() && this == serialExecutorMap.get(identifier)) {
                            serialExecutorMap.remove(identifier);
                            terminating.signalAll();
                            if (state == State.SHUTDOWN && serialExecutorMap.isEmpty()) {
                                executor.shutdown();
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }

            /**
             * Returns true if the list is empty and there is no task currently executing.
             */
            public boolean isEmpty() {
                lock.lock();
                try {
                    return active == null && tasks.isEmpty();
                } finally {
                    lock.unlock();
                }
            }
        }
    }
}
