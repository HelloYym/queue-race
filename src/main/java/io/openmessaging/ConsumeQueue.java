package io.openmessaging;

import io.openmessaging.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/6/26
 * Time: 下午2:09
 */
public class ConsumeQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /*CQ中每个单元的大小*/
    public static final int CQ_STORE_UNIT_SIZE = 12;

    private final MappedFileQueue mappedFileQueue;
    private final String queueName;
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;

    /*逻辑队列起始offset，暂时没有用*/
    private volatile long minLogicOffset = 0;

    public ConsumeQueue(
            final String queueName,
            final String storePath,
            final int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.queueName = queueName;

        String queueDir = this.storePath + File.separator + queueName;
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, false);
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
    }

    /*public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.queueName + " " + (result ? "OK" : "Failed"));
        return result;
    }*/

    /*同步添加消息索引*/
    public synchronized boolean putMessagePositionInfo(final long offset, final int size, final long cqOffset) {

        // 如果已经重放过，直接返回成功
        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);

        // 计算consumeQueue存储位置，并获得对应的MappedFile
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);


            /*如果没有文件或最后一个文件写满，新建一个文件*/
        if (null == mappedFile || mappedFile.isFull() || mappedFile.getWrotePosition() + CQ_STORE_UNIT_SIZE > this.mappedFileSize) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
        }

        if (mappedFile != null) {

            /*校验consumeQueue存储位置是否合法*/
            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} QName: {} Diff: {}",
                            expectLogicOffset, currentLogicOffset, this.queueName, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} QName: {} Diff: {}",
                            expectLogicOffset,
                            currentLogicOffset,
                            this.queueName,
                            expectLogicOffset - currentLogicOffset
                    );
                }
            }
            this.maxPhysicOffset = offset;
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }


        return false;
    }


    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {

        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;

        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
        }

        return null;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getQueueName() {
        return queueName;
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }



    /*文件刷盘*/
    public boolean flush(final int flushLeastPages) {
        return this.mappedFileQueue.flush(flushLeastPages);
    }
//    public void destroy() {
//        this.maxPhysicOffset = -1;
//        this.minLogicOffset = 0;
//        this.mappedFileQueue.destroy();
//    }
    /*获取最后一个unit的物理地址的结尾*/
//    public long getLastOffset() {
//        long lastOffset = -1;
//
//        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
//        if (mappedFile != null) {
//
//            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
//            if (position < 0)
//                position = 0;
//
//            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
//            byteBuffer.position(position);
//            for (int i = 0; i < this.mappedFileSize; i += CQ_STORE_UNIT_SIZE) {
//                long offset = byteBuffer.getLong();
//                int size = byteBuffer.getInt();
//
//                if (offset >= 0 && size > 0) {
//                    lastOffset = offset + size;
//                } else {
//                    break;
//                }
//            }
//        }
//        return lastOffset;
//    }
}
