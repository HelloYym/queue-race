package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-04
 * Time: 下午3:28
 */
public class QueueIndex {

    private List<Long> index = new ArrayList<>();

    private int gap;

    public QueueIndex(int gap) {
        this.gap = gap;
    }

    public void putIndex(long offset) {
        index.add(offset);
    }

    public long getIndex(int offset) {
        if (offset / gap < index.size())
            return index.get(offset / gap);
        else
            return -1;
    }
}
