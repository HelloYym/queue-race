package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.openmessaging.config.MessageStoreConfig.MAX_QUEUE_NUM;
import static io.openmessaging.config.MessageStoreConfig.SparseSize;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-06-26
 */

class DefaultMessageStore {

    private final MessageStoreConfig messageStoreConfig;

    static final QueueIndex[] queueIndexTable = new QueueIndex[MAX_QUEUE_NUM];

    //    static final QueueCache[] queueMsgCache = new QueueCache[MAX_QUEUE_NUM];
    private static  DirectQueueCache[] queueMsgCache = new DirectQueueCache[MAX_QUEUE_NUM];
//    private static  ReadQueueCache[] readMsgCache = new ReadQueueCache[MAX_QUEUE_NUM];

//    private Lock[] queueLock = new Lock[MAX_QUEUE_NUM];

    private AtomicBoolean[] queueLock = new AtomicBoolean[MAX_QUEUE_NUM];


    private static final int numCommitLog = 200;

    private final ArrayList<CommitLogLite> commitLogList;

    private final AtomicBoolean consumeStart = new AtomicBoolean(false);

    private boolean flushComplete = false;

    DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.commitLogList = new ArrayList<>();
        for (int i = 0; i < numCommitLog; i++)
            this.commitLogList.add(new CommitLogLite(1024 * 1024 * 1024, getMessageStoreConfig().getStorePathCommitLog()));

        for (int topicId = 0; topicId < MAX_QUEUE_NUM; topicId++){
//            queueLock[topicId] = new ReentrantLock();
            queueLock[topicId] = new AtomicBoolean(false);
            queueMsgCache[topicId] = new DirectQueueCache();
//            readMsgCache[topicId] = new ReadQueueCache(queueMsgCache[topicId].getByteBuffer());
        }
    }

    private CommitLogLite getCommitLog(int topicId) {
        return commitLogList.get(topicId % numCommitLog);
    }

    void putMessage(int topicId, byte[] msg) {
        DirectQueueCache cache = queueMsgCache[topicId];
        int size = cache.addMessage(msg);
        if (size == SparseSize) {
            int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
            queueIndexTable[topicId].putIndex(offset);
            cache.clear();
        }
    }

//    private void flushCache() {
//        for (int topicId = 0; topicId < MAX_QUEUE_NUM; topicId++){
//            flushCache(topicId);
//        }
////        queueMsgCache = null;
//        flushComplete = true;
//    }

    private void flushCache(int topicId) {
        DirectQueueCache cache = queueMsgCache[topicId];
        int size = cache.getSize();
        if (size == 0) return;
        if (size < SparseSize) cache.putTerminator();
        int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
        queueIndexTable[topicId].putIndex(offset);
        cache.clear();
    }

    List<byte[]> getMessage(int topicId, int offset, int maxMsgNums) {

//        if (consumeStart.compareAndSet(false, true)) {
//            flushCache();
//        } else {
//            while (!flushComplete) {
//                try {
//                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }

        int off = offset;
        int nums = maxMsgNums;
        QueueIndex index = queueIndexTable[topicId];
        CommitLogLite commitLog = getCommitLog(topicId);
        List<byte[]> msgList = new ArrayList<>(maxMsgNums);

        if (queueLock[topicId].compareAndSet(false, true)) {

            flushCache(topicId);

            while (nums > 0 && index.getIndex(off) != -1) {
                int start = off % SparseSize;
                int end = Math.min(start + nums, SparseSize);
                try {
                    DirectQueueCache cache = queueMsgCache[topicId];
                    int phyOffset = index.getIndex(off);
//                    if (cache.getOffset() != phyOffset) commitLog.getMessage(phyOffset, cache);
                    commitLog.getMessage(phyOffset, cache);
                    msgList.addAll(cache.getMessage(start, end));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                nums -= (end - start);
                off += (end - start);
            }
            queueMsgCache[topicId] = null;
//            queueLock[topicId].unlock();
        } else {
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

        return msgList;
    }

    private MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
