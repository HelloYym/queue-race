package io.openmessaging.config;

import io.openmessaging.ConsumeQueue;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/6/23
 * Time: 下午8:11
 */
public class MessageStoreConfig {
    //The root directory in which the log data is kept
    private String storePathRootDir = "alidata1/race2018/data";

    //稀疏索引，每存多少个写一个索引
    public static final int SparseSize = 16;//每隔20个存一次

    //The directory in which the commitlog is kept
    private String storePathCommitLog = storePathRootDir + File.separator + "commitlog";

    // CommitLog file size,default is 1G
    private int mapedFileSizeCommitLog = 1024 * 1024 * 1024;

    // ConsumeQueue file size,default is 30W
//    private int mapedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;

    private int mapedFileSizeConsumeQueue = 10000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;

    // CommitLog flush interval
    // flush data to disk
    private int flushIntervalCommitLog = 500;

    // Whether schedule flush,default is real-time
    private boolean flushCommitLogTimed = false;

    // ConsumeQueue flush interval
    private int flushIntervalConsumeQueue = 1000;

    private int maxMessageSize = 1024 * 1024 * 4;

    // How many pages are to be flushed when flush ConsumeQueue
    private int flushConsumeQueueLeastPages = 2;

    private int flushConsumeQueueThoroughInterval = 1000 * 60;

    // How many pages are to be flushed when flush CommitLog
    private int flushCommitLogLeastPages = 4;
    private int flushCommitLogThoroughInterval = 1000 * 10;
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    private int maxTransferCountOnMessageInMemory = 32;
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    private int maxTransferCountOnMessageInDisk = 8;
    private int accessMessageInMemoryMaxRatio = 40;

    public int getMapedFileSizeCommitLog() {
        return mapedFileSizeCommitLog;
    }

    public int getMapedFileSizeConsumeQueue() {

        int factor = (int) Math.ceil(this.mapedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return (int) (factor * ConsumeQueue.CQ_STORE_UNIT_SIZE);
    }

    public int getSparseSize() {
        return SparseSize;
    }

    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }

    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }

    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }

    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }

    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }


    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }
}
