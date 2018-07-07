package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.config.MessageStoreConfig.SparseSize;
import static io.openmessaging.config.MessageStoreConfig.QUEUE_CACHE_SIZE;


/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/7
 * Time: 下午4:04
 */
public class ReadQueueCache {

    private final AtomicInteger refCount = new AtomicInteger(0);

    private ByteBuffer byteBuffer;

    private int offset = -1;

    public ReadQueueCache(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getWriteBuffer() {
        byteBuffer.clear();
        return byteBuffer;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    ArrayList<byte[]> getMessage(int start, int end) {

        ArrayList<byte[]> msgList = new ArrayList<>();

        byteBuffer.position(0);
        byteBuffer.limit(QUEUE_CACHE_SIZE);

//        int currentPos = 0;
        int idx = 0;
        byte size;

        while (idx < end){
            /*读取消息长度*/
//            byteBuffer.position(currentPos);
            size = byteBuffer.get();

            if (size == 0) break;

            /*读取消息体*/
            byte[] msg = new byte[size];
//            byteBuffer.position(currentPos+1);
            byteBuffer.get(msg, 0, size);

            if (idx >= start)
                msgList.add(msg);

//            currentPos += 1 + size;
            idx++;
        }

        return msgList;
    }

}