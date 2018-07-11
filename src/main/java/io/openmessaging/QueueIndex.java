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

    private int[] index;

    private int size = 0;

    private int cacheSize;

    public QueueIndex(int cacheSize) {
        this.cacheSize = cacheSize;
        index = new int[2100 / cacheSize];
    }

    public int getSize() {
        return size;
    }

    public void putIndex(int offset) {
        index[size++] = offset;
    }

    public int getIndex(int offset) {
        if (offset / cacheSize < size)
            return index[offset / cacheSize];
        else
            return -1;
    }
}