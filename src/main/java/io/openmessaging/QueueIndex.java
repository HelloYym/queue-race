package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.config.MessageStoreConfig.SparseSize;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-04
 * Time: 下午3:28
 */
public class QueueIndex {

    private long[] index = new long[2100 / SparseSize];

    private int size = 0;

    public QueueIndex() {
    }

    public void putIndex(long offset) {
        index[size ++] = offset;
    }

    public long getIndex(int offset) {
        if (offset / SparseSize < size)
            return index[offset / SparseSize];
        else
            return -1;
    }
}
