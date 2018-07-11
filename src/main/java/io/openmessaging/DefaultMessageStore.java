package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.openmessaging.config.MessageStoreConfig.*;


/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-06-26
 */

class DefaultMessageStore {

    private final MessageStoreConfig messageStoreConfig;

    /** 每个队列存储索引 **/
    private QueueIndex[] queueIndexTable = new QueueIndex[MAX_QUEUE_NUM];

    /** 每个队列缓存（directedbuffer） **/
    private DirectQueueCache[] queueMsgCache = new DirectQueueCache[MAX_QUEUE_NUM];

    /** 最后未刷盘的缓存存到堆内内存中 **/
    private QueueCache[] queueCache = new QueueCache[MAX_QUEUE_NUM];

    /** 为了区分索引检查和消费阶段 **/
    private AtomicBoolean[] queueLock = new AtomicBoolean[MAX_QUEUE_NUM];

    /** numCommitLog个消息存储对象 **/
    private final CommitLogLite[] commitLogList = new CommitLogLite[numCommitLog];

    /** 判断每个队列最后的消息是否从directedbuffer存到了queueMsgCache中 **/
    private boolean[] flushComplete = new boolean[MAX_QUEUE_NUM];

    /** 判断消费是否开始 **/
    private final AtomicBoolean consumeStart = new AtomicBoolean(false);

    /** 索引检查阶段使用directedbuffer进行读取，防止多线程竞争需要上锁 **/
    private Lock[] bufferLock = new ReentrantLock[MAX_QUEUE_NUM];

    /** 初始化 **/
    DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;

        for (int i = 0; i < numCommitLog; i++) {
            commitLogList[i] = (new CommitLogLite(messageStoreConfig.getFileSizeCommitLog(), getMessageStoreConfig().getStorePathCommitLog()));
        }

        for (int topicId = 0; topicId < MAX_QUEUE_NUM; topicId++) {
            queueLock[topicId] = new AtomicBoolean(false);
            queueMsgCache[topicId] = new DirectQueueCache(SparseSize);
            queueIndexTable[topicId] = new QueueIndex();
            queueCache[topicId] = new QueueCache();
            bufferLock[topicId] = new ReentrantLock(false);
        }
    }

    /** 保证100w个队列平均分配到numCommilog个文件中 **/
    private CommitLogLite getCommitLog(int index) {
//        return commitLogList[index % QUEUE_NUM_PER_COMMITLOG];
        int idx = index / QUEUE_NUM_PER_COMMITLOG;
        return commitLogList[idx < 60 ? idx : 59];
    }

    /** 写消息到文件 **/
    void putMessage(int topicId, byte[] msg) {
        DirectQueueCache cache = queueMsgCache[topicId];//directedbuffer消息缓存
        int size = cache.addMessage(msg);//将消息写入缓存
        //如果其中一个队列的消息数达到了SparseSize，将缓存写入文件并将缓存的索引存入索引缓存中。
        if (size == SparseSize) {
            int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
            queueIndexTable[topicId].putIndex(offset);
            cache.clear();
        }
    }

    /** 将最后缓存中未刷盘的消息刷到内存中 **/
    private void flushAll() {
        for (int i = 0; i < MAX_QUEUE_NUM; i++) {
            flushCache(i);
            flushComplete[i] = true;
        }
    }
    private void flushCache(int topicId) {
        DirectQueueCache cache = queueMsgCache[topicId];
        int size = cache.getSize();
        if (size == 0) return;
        queueCache[topicId].getMsgList().addAll(cache.getMessage(0, size));
        cache.clear();
    }


    /** 索引与消费消息 **/
    List<byte[]> getMessage(int topicId, int offset, int maxMsgNums) {
        //将最后未刷盘消息刷入内存
        if (consumeStart.compareAndSet(false, true)) {
            flushAll();
        } else {
            while (!flushComplete[topicId]) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        int off = offset;
        int nums = maxMsgNums;
        QueueIndex index = queueIndexTable[topicId];
        CommitLogLite commitLog = getCommitLog(topicId);

        List<byte[]> msgList = new ArrayList<>(maxMsgNums);

        /*如果offset==0可以认为是消费开始*/
        if (offset == 0) queueLock[topicId].set(true);

        //索引检查
        if (!queueLock[topicId].get()) {
            bufferLock[topicId].lock();

            try {
                DirectQueueCache cache = queueMsgCache[topicId];
                while (nums > 0 && index.getIndex(off) != -1) {
                    int start = off % SparseSize;
                    int end = Math.min(start + nums, SparseSize);
                    int phyOffset = index.getIndex(off);
                    try {
                        commitLog.getMessage(phyOffset, cache, start, end);
                        msgList.addAll(cache.getMessage(start, end));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    nums -= (end - start);
                    off += (end - start);
                }
            } finally {
                bufferLock[topicId].unlock();
            }
        }

        //消费
        else {
            while (nums > 0 && index.getIndex(off) != -1) {
                int start = off % SparseSize;
                int end = Math.min(start + nums, SparseSize);
                try {
                    int phyOffset = index.getIndex(off);
                    msgList.addAll(commitLog.getMessage(phyOffset, start, end));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                nums -= (end - start);
                off += (end - start);
            }
        }

        //无论是消费还是索引检查都可能进行这个步骤。可能需检查的部分消息在内存中
        if (nums > 0) {
            msgList.addAll(queueCache[topicId].getMsgList(off % SparseSize,
                    Math.min(off % SparseSize + nums, queueCache[topicId].size())));
        }
        return msgList;
    }

    private MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
