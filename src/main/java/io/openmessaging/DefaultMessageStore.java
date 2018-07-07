package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
    static final DirectQueueCache[] queueMsgCache = new DirectQueueCache[MAX_QUEUE_NUM];

    private static final int numCommitLog = 200;

    private final ArrayList<CommitLogLite> commitLogList;

    private final AtomicBoolean consumeStart = new AtomicBoolean(false);
    private boolean flushComplete = false;

    DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.commitLogList = new ArrayList<>();
        for (int i = 0; i < numCommitLog; i++)
            this.commitLogList.add(new CommitLogLite(1024 * 1024 * 1024, getMessageStoreConfig().getStorePathCommitLog()));

        for (int topicId = 0; topicId < MAX_QUEUE_NUM; topicId++)
            queueMsgCache[topicId] = new DirectQueueCache();
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

    private void flushCache() {
        for (int topicId = 0; topicId < MAX_QUEUE_NUM; topicId++){
            DirectQueueCache cache = queueMsgCache[topicId];
            int size = cache.getSize();
            if (size == 0) continue;
            if (size < SparseSize) cache.putTerminator();
            int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
            queueIndexTable[topicId].putIndex(offset);
            cache.clear();
        }
        flushComplete = true;
    }

    List<byte[]> getMessage(int topicId, int offset, int maxMsgNums) {

        if (consumeStart.compareAndSet(false, true)){
            flushCache();
        } else {
            while (!flushComplete){
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

        while (nums > 0 && index.getIndex(off) != -1) {
            int start = off % SparseSize;
            int end = Math.min(start + nums, SparseSize) - 1;
            try {
                msgList.addAll(commitLog.getMessage((int) index.getIndex(off), start, end));
            } catch (Exception e) {
                e.printStackTrace();
            }

            nums -= (end - start + 1);
            off += (end - start + 1);
        }

        return msgList;
    }

    private MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
