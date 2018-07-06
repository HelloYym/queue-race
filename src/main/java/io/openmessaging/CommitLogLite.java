package io.openmessaging;

import io.openmessaging.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.config.MessageStoreConfig.SparseSize;

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
    public int putMessage(List<byte[]> msgs) {

        int bodyLength = 0;
        for (byte[] msg : msgs)
            bodyLength += msg.length;
        int totalLength = SparseSize + bodyLength;

        int currentPos = this.wrotePosition.getAndAdd(totalLength);

        if (currentPos < this.mappedFileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);

            for (byte[] msg : msgs)
                byteBuffer.put((byte) msg.length);

            for (byte[] msg : msgs)
                byteBuffer.put(msg);

            return currentPos;
        } else
            return -1;
    }

//    public ArrayList<byte[]> getMessage(int offset, int start, int end) {
//
//        ArrayList<byte[]> msgList = new ArrayList<>();
//
//        int indexPos = offset;
//        int msgPos = offset + SparseSize;
//
//        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
//
//        for (int i = 0; i < start; i++) {
//            byteBuffer.position(indexPos);
//            msgPos += byteBuffer.get();
//            indexPos++;
//        }
//
//        for (int i = start; i <= end; i++) {
//
//            /*读取消息长度*/
//            byteBuffer.position(indexPos);
//            int size = byteBuffer.get();
//
//            /*读取消息体*/
//            byte[] msg = new byte[size];
//            byteBuffer.position(msgPos);
//            byteBuffer.get(msg, 0, size);
//
//            msgList.add(msg);
//
//            msgPos += size;
//            indexPos++;
//        }
//
//        return msgList;
//    }


    //写一串数据
    public int putMessage(ByteBuffer byteBuffer) {

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

        int currentPos = offset;
        int idx = 0;
        byte size;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();

        while (idx <= end){
            /*读取消息长度*/
            byteBuffer.position(currentPos);
            size = byteBuffer.get();

            /*读取消息体*/
            byte[] msg = new byte[size];
            byteBuffer.position(currentPos+1);
            byteBuffer.get(msg, 0, size);

            if (idx >= start)
                msgList.add(msg);

            currentPos += 1 + size;
            idx++;
        }

        return msgList;
    }

}













