package io.openmessaging;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by IntelliJ IDEA.
 * User: QWC
 * Date: 2018/6/26
 * Time: 上午9:31
 */
public class PutMessageSpinLock {
    //true: Can lock, false : in lock.
    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    public void lock() {
        boolean flag;
        do {
            flag = this.putMessageSpinLock.compareAndSet(true, false);
        }
        while (!flag);
    }

    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }
}
