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
    /** 消息存储路径 **/
    private String storePathCommitLog = "alidata1/race2018/data";

    /** 稀疏索引，每存多少个写一个索引 **/
    public static final int SparseSize = 20;

    /** 因为题目说每个消息大小为50字节左右，我们将每个消息大小设为59，不足59部分为空 **/
    public static final int MESSAGE_SIZE = 59;

    /** 最大队列数量 **/
    public static final int MAX_QUEUE_NUM = 1000100;

    /** 每个队列最大消息数量 **/
    public static final int MAX_MESSAGE_NUM_PER_QUEUE = 2100;

    /** 因为每隔SparseSize大小消息存储一个索引，这个是每个队列最大的索引数量 **/
    public static final int INDEX_NUM = MAX_MESSAGE_NUM_PER_QUEUE / SparseSize;

    /** 每个SparseSize中第i个消息的起始偏移 **/
    public static int[] numbers = new int[SparseSize + 1];
    static {
        for (int i = 0; i <= SparseSize; i++) numbers[i] = i * MESSAGE_SIZE;
    }

    /** 消息存储文件的最大值2g-1 **/
    private int fileSizeCommitLog = Integer.MAX_VALUE;

    /** 存储消息的文件数量 **/
    public static final int numCommitLog = 60;

    /** 每个文件中所存队列的数量 **/
    public static final int QUEUE_NUM_PER_COMMITLOG = 16667;

    public int getFileSizeCommitLog() {
        return fileSizeCommitLog;
    }
    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

}
