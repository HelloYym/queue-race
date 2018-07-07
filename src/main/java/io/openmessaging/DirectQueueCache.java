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
        return byteBuffer;
    }

    void putTerminator(){
        byteBuffer.put((byte) 0);
    }


    public byte getSize() {
        return size;
    }

    void clear() {
        /*position设为0，limit设为capacity，回到写模式*/
        byteBuffer.clear();
        size = 0;
    }
}
