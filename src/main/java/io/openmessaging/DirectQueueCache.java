package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.config.MessageStoreConfig.QUEUE_CACHE_SIZE;


/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/6
 * Time: 下午8:15
 */
class DirectQueueCache {

    private ByteBuffer byteBuffer;

    private ArrayList<byte[]> msgList;

    /*是个瓶颈*/
    private byte size = 0;

    DirectQueueCache() {
        this.byteBuffer = ByteBuffer.allocateDirect(QUEUE_CACHE_SIZE);
    }

    int addMessage(byte[] msg) {
        byteBuffer.put((byte) msg.length);
        byteBuffer.put(msg);
        return ++size;
    }

    ByteBuffer getByteBuffer() {
        /*limit设为当前position,position设为0*/
        byteBuffer.flip();
        return byteBuffer;
    }

    ArrayList<byte[]> readMsgList() {

        if (msgList != null)
            return msgList;

        msgList = new ArrayList<>();
        byteBuffer.flip();

        while (byteBuffer.hasRemaining()) {
            /*读取消息长度*/
            byte len = byteBuffer.get();
            /*读取消息体*/
            byte[] msg = new byte[len];
            byteBuffer.get(msg);
            msgList.add(msg);
        }

        return msgList;
    }

    void clear() {
        /*position设为0，limit设为capacity，回到写模式*/
        byteBuffer.clear();
        size = 0;
    }
}
