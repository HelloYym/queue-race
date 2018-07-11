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
    public static final int SparseSize = 20;

    public static final int MESSAGE_SIZE = 60;

    public static final int MAX_QUEUE_NUM = 1000100;

    public static final int MAX_MESSAGE_NUM_PER_QUEUE = 2100;

    public static final int INDEX_NUM = MAX_MESSAGE_NUM_PER_QUEUE / SparseSize;

    public static int[] numbers = new int[SparseSize + 1];

    static {
        for (int i = 0; i <= SparseSize; i++) numbers[i] = i * MESSAGE_SIZE;
    }

    // CommitLog file size,default is 1G
    private int fileSizeCommitLog = Integer.MAX_VALUE;

    public static final int numCommitLog = 60;

    //The directory in which the commitlog is kept
    private String storePathCommitLog = storePathRootDir + File.separator + "commitlog";

    public int getFileSizeCommitLog() {
        return fileSizeCommitLog;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

}
