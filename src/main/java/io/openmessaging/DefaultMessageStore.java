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

    private ReadPointer[] readPointers = new ReadPointer[MAX_QUEUE_NUM];

    private int numCommitLog;

    private final ArrayList<CommitLogLite> commitLogList;

    private final AtomicBoolean consumeStart = new AtomicBoolean(false);

    DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.commitLogList = new ArrayList<>();
        this.numCommitLog = messageStoreConfig.getNumCommitLog();
        for (int i = 0; i < numCommitLog; i++)
            this.commitLogList.add(new CommitLogLite(messageStoreConfig.getFileSizeCommitLog(), getMessageStoreConfig().getStorePathCommitLog()));

        for (int topicId = 0; topicId < MAX_QUEUE_NUM; topicId++){
            queueLock[topicId] = new AtomicBoolean(false);
            queueMsgCache[topicId] = new DirectQueueCache();
            queueIndexTable[topicId] = new QueueIndex();
            readPointers[topicId] = new ReadPointer();
        }
    }

    private CommitLogLite getCommitLog(int index) {
//        return commitLogList.get(index);
        return commitLogList.get(index % numCommitLog);
    }

    void putMessage(int topicId, byte[] msg) {
        DirectQueueCache cache = queueMsgCache[topicId];
        int size = cache.addMessage(msg);
        if (size == SparseSize) {
//            int offset = getCommitLog(queueIndexTable[topicId].getSize()).putMessage(cache.getByteBuffer());
            int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
            queueIndexTable[topicId].putIndex(offset);
            cache.clear();
        }
    }

    private void flushCache(int topicId) {
        DirectQueueCache cache = queueMsgCache[topicId];
        int size = cache.getSize();
        if (size == 0) return;
        if (size < SparseSize) cache.putTerminator();
//        int offset = getCommitLog(queueIndexTable[topicId].getSize()).putMessage(cache.getByteBuffer());
        int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
        queueIndexTable[topicId].putIndex(offset);
        cache.clear();
    }

    List<byte[]> getMessage(int topicId, int offset, int maxMsgNums) {

        int off = offset;
        int nums = maxMsgNums;
        QueueIndex index = queueIndexTable[topicId];
        CommitLogLite commitLog = getCommitLog(topicId);

        List<byte[]> msgList = new ArrayList<>(maxMsgNums);

        if (queueLock[topicId].compareAndSet(false, true)) {

            flushCache(topicId);

            while (nums > 0 && index.getIndex(off) != -1) {
//                CommitLogLite commitLog = getCommitLog(off / SparseSize);
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
//            queueMsgCache[topicId].munlock();
            queueMsgCache[topicId] = null;
//            queueLock[topicId].unlock();
        } else {
            while (nums > 0 && index.getIndex(off) != -1) {
//                CommitLogLite commitLog = getCommitLog(off / SparseSize);
                int start = off % SparseSize;
                int end = Math.min(start + nums, SparseSize);
                try {
                    int phyOffset = index.getIndex(off);
                    msgList.addAll(commitLog.getMessage(phyOffset, start, end, readPointers[topicId]));
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
