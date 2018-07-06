package io.openmessaging.config;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    public static final int MAX_QUEUE_NUM = 1100000;

    public static final int MAX_MESSAGE_NUM_PER_QUEUE = 2200;

    public static final Lock lock = new ReentrantLock();

    //The directory in which the commitlog is kept
    private String storePathCommitLog = storePathRootDir + File.separator + "commitlog";

    // CommitLog file size,default is 1G
    private int mapedFileSizeCommitLog = 1024 * 1024 * 1024;

    // CommitLog flush interval
    // flush data to disk
    private int flushIntervalCommitLog = 500;

    private int maxMessageSize = 1024 * 1024 * 4;

    // How many pages are to be flushed when flush CommitLog
    private int flushCommitLogLeastPages = 4;
    private int flushCommitLogThoroughInterval = 1000 * 10;

    public int getMapedFileSizeCommitLog() {
        return mapedFileSizeCommitLog;
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

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

}
