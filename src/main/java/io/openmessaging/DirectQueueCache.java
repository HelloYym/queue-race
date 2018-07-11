package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static io.openmessaging.config.MessageStoreConfig.MESSAGE_SIZE;
import static io.openmessaging.config.MessageStoreConfig.numbers;
import static io.openmessaging.utils.UnsafeUtil.UNSAFE;


/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/6
 * Time: 下午8:15
 */

/** 消息缓存类，使用的是directedbuffer **/
class DirectQueueCache {

    private ByteBuffer byteBuffer;

    private final long address;

    private byte size = 0;

    /** directedbuffer初始化 **/
    DirectQueueCache(int cacheSize) {
        this.byteBuffer = ByteBuffer.allocateDirect(cacheSize * MESSAGE_SIZE);
        this.address = ((DirectBuffer) byteBuffer).address();
    }

    /** 向缓存中添加消息 **/
    int addMessage(byte[] msg) {
        long pos = address + numbers[size];
        //注意！！！！可能每个消息长度不一致，存储时要有一个字节用于存储消息长度
        UNSAFE.putByte(pos, (byte) msg.length);
        //写入消息
        for (int i = 0; i < msg.length; i++) {
            UNSAFE.putByte(pos + i + 1, msg[i]);
        }
        byteBuffer.position(numbers[++size]);
        return size;
    }

    /** 从directedbuffer读取消息 **/
    ArrayList<byte[]> getMessage(int start, int end) {

        ArrayList<byte[]> msgList = new ArrayList<>();

//        for (int i = start; i < end; i++) {
//            byteBuffer.position(i * MESSAGE_SIZE);
//            byte size = byteBuffer.get();
//            if (size == 0) break;
//            byte[] msg = new byte[size];
//            byteBuffer.get(msg, 0, size);
//            msgList.add(msg);
//        }

        /** Unsafe **/

        long pos = address + numbers[start];
        for (int i = start; i < end; i++, pos += MESSAGE_SIZE) {
            byte size = UNSAFE.getByte(pos);
            if (size == 0) break;
            byte[] msg = new byte[size];
            for (int j = 0; j < size; j++) {
                msg[j] = UNSAFE.getByte(pos + j + 1);
            }
            msgList.add(msg);
        }

        return msgList;
    }

    /*position设为0，limit设为capacity，回到写模式*/
    void clear() {
        byteBuffer.clear();
        size = 0;
    }
    public ByteBuffer getWriteBuffer() {
        byteBuffer.clear();
        return byteBuffer;
    }

    ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public byte getSize() {
        return size;
    }
}
