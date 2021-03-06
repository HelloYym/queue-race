package io.openmessaging;

import static io.openmessaging.config.MessageStoreConfig.INDEX_NUM;
import static io.openmessaging.config.MessageStoreConfig.SparseSize;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-04
 * Time: 下午3:28
 */

/** 存储每个队列的索引 **/
public class QueueIndex {

    private int[] index;

    private int size = 0;

    public QueueIndex() {
        index = new int[INDEX_NUM];
    }

    public int getSize() {
        return size;
    }

    public void putIndex(int offset) {
        index[size++] = offset;
    }

    public int getIndex(int offset) {
        int off = offset / SparseSize;
        if (off < size)
            return index[off];
        else
            return -1;
    }
}