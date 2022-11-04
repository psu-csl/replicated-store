package log;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class InstanceWithCv {
    private Instance instance;
    private final ReentrantLock mu = new ReentrantLock();
    private final Condition cv = mu.newCondition();

    public InstanceWithCv(Instance instance) {
        this.instance = instance;
    }

    public InstanceWithCv() {
        this.instance = null;
    }

    public Instance getInstance() {
        return this.instance;
    }

    public void setInstance(Instance instance) {
        this.instance = instance;
    }

    public void await() throws InterruptedException {
        this.cv.await();
    }

    public void signal() {
        this.mu.lock();
        this.cv.signal();
        this.mu.unlock();
    }
}
