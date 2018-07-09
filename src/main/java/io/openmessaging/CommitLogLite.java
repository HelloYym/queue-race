package io.openmessaging;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import io.openmessaging.common.LoggerName;
import io.openmessaging.utils.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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


/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/5
 * Time: 下午11:04
 */

public class CommitLogLite {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /*文件的大小*/
    private final int mappedFileSize;

    /*映射的文件名*/
    private static AtomicInteger fileName = new AtomicInteger(0);

    /*映射的内存对象*/
    private MappedByteBuffer mappedByteBuffer;

    /*映射的fileChannel对象*/
    private FileChannel fileChannel;

    /*文件尾指针*/
    private AtomicInteger wrotePosition = new AtomicInteger(0);

    public CommitLogLite(int mappedFileSize, String storePath) {
        this.mappedFileSize = mappedFileSize;

        /*检查文件夹是否存在*/
        ensureDirOK(storePath);

        /*打开文件，并将文件映射到内存*/
        try {
            File file = new File(storePath + File.separator + fileName.getAndIncrement());
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedFileSize);

//            warmup();

        } catch (FileNotFoundException e) {
            log.error("create file channel " + fileName + " Failed. ", e);
        } catch (IOException e) {
            log.error("map file " + fileName + " Failed. ", e);
        }
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    //写一串数据
    int putMessage(ByteBuffer byteBuffer) {

        byteBuffer.flip();
        int currentPos = this.wrotePosition.getAndAdd(byteBuffer.limit());
        try {
            this.fileChannel.write(byteBuffer, currentPos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return currentPos;
    }

    ArrayList<byte[]> getMessage(int offset, int start, int end) {

        ArrayList<byte[]> msgList = new ArrayList<>();

        int pos = offset;
        int idx = 0;


        byte size;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();

        while (idx < end){
            /*读取消息长度*/
            byteBuffer.position(pos);
            size = byteBuffer.get();

            if (size == 0) break;

            /*读取消息体*/
            byte[] msg = new byte[size];
            byteBuffer.position(pos+1);
            byteBuffer.get(msg, 0, size);

            if (idx >= start)
                msgList.add(msg);

            pos += 1 + size;
            idx++;
        }

        return msgList;
    }

    ArrayList<byte[]> getMessage(int offset, int start, int end, ReadPointer readPointer) {

        ArrayList<byte[]> msgList = new ArrayList<>();

        int pos = offset;
        int idx = 0;

        if (readPointer.getCurrentOffset() == offset && readPointer.getCurrentIndex() == start) {
            pos = readPointer.getCurrentPos();
            idx = readPointer.getCurrentIndex();
        }

        byte size;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();

        while (idx < end){
            /*读取消息长度*/
            byteBuffer.position(pos);
            size = byteBuffer.get();

            if (size == 0) break;

            /*读取消息体*/
            byte[] msg = new byte[size];
            byteBuffer.position(pos+1);
            byteBuffer.get(msg, 0, size);

            if (idx >= start)
                msgList.add(msg);

            pos += 1 + size;
            idx++;
        }

        readPointer.setCurrentIndex(idx);
        readPointer.setCurrentPos(pos);
        readPointer.setCurrentOffset(offset);
        return msgList;
    }


    void getMessage(int offset, DirectQueueCache cache) {

        try {
            fileChannel.read(cache.getWriteBuffer(), offset);
//            cache.setOffset(offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void warmup(){
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
//        LibC.INSTANCE.mlock(pointer, new NativeLong(this.mappedFileSize));
        LibC.INSTANCE.madvise(pointer, new NativeLong(this.mappedFileSize), LibC.MADV_WILLNEED);

    }


}














