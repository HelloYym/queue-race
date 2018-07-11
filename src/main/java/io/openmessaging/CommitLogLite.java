package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.config.MessageStoreConfig.MESSAGE_SIZE;
import static io.openmessaging.config.MessageStoreConfig.numbers;
import static io.openmessaging.utils.UnsafeUtil.UNSAFE;

//import static io.openmessaging.config.MessageStoreConfig.QUEUE_CACHE_SIZE;

/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/5
 * Time: 下午11:04
 */

/** 消息存储文件类 **/
public class CommitLogLite {

    /** 文件的大小 **/
    private final int mappedFileSize;

    /** 映射的文件名 **/
    private static AtomicInteger fileName = new AtomicInteger(0);

    /** 映射的内存对象 **/
    private MappedByteBuffer mappedByteBuffer;

    /** 映射的fileChannel对象 **/
    private FileChannel fileChannel;

    /** 文件尾指针 **/
    private AtomicInteger wrotePosition = new AtomicInteger(0);

    public CommitLogLite(int mappedFileSize, String storePath) {
        this.mappedFileSize = mappedFileSize;

        /** 检查文件夹是否存在 **/
        ensureDirOK(storePath);

        /** 打开文件，并将文件映射到内存 **/
        try {
            File file = new File(storePath + File.separator + fileName.getAndIncrement());
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, mappedFileSize);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** 检查文件夹是否存在 **/
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
            }
        }
    }

    /** 将directedbuffer缓存中数据写入文件 **/
    int putMessage(ByteBuffer byteBuffer) {
        byteBuffer.flip();
        int currentPos = this.wrotePosition.getAndAdd(byteBuffer.limit());//避免多线程竞争
        try {
            this.fileChannel.write(byteBuffer, currentPos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return currentPos;
    }

    /** 应用于消费阶段，从mappedbuffer使用unsafe方式读取 **/
    /** offset为物理存储偏移值，start、end为要get的始终 **/
    ArrayList<byte[]> getMessage(int offset, int start, int end) {

        ArrayList<byte[]> msgList = new ArrayList<>();

//        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
//
//        for (int i = start; i < end; i++) {
//            byteBuffer.position(offset + i * MESSAGE_SIZE);
//            byte size = byteBuffer.get();
//            if (size == 0) break;
//            byte[] msg = new byte[size];
//            byteBuffer.get(msg, 0, size);
//            msgList.add(msg);
//        }

        /** Unsafe读取 **/
        long pos = ((DirectBuffer) mappedByteBuffer).address() + offset + numbers[start];
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

    /** 应用于索引检查，从文件中读取到directedbuffer **/
    /** offset为物理存储偏移值，cache为directedbuffer缓存，start、end为要get的始终 **/
    void getMessage(int offset, DirectQueueCache cache, int start, int end) {
        try {
            ByteBuffer byteBuffer = cache.getWriteBuffer();
            byteBuffer.position(numbers[start]);
            byteBuffer.limit(numbers[end]);
            fileChannel.read(byteBuffer, offset + numbers[start]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}














