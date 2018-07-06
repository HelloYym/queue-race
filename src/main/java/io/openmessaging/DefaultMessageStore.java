package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.List;

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

    private static final int numCommitLog = 100;

    private final ArrayList<CommitLogLite> commitLogList;

    DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.commitLogList = new ArrayList<>();
        for (int i = 0; i < numCommitLog; i++)
            this.commitLogList.add(new CommitLogLite(Integer.MAX_VALUE, getMessageStoreConfig().getStorePathCommitLog()));
    }

    private CommitLogLite getCommitLog(int topicId){
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

    List<byte[]> getMessage(int topicId, int offset, int maxMsgNums) {

        int off = offset;
        int nums = maxMsgNums;
        QueueIndex index = queueIndexTable[topicId];
        CommitLogLite commitLog = getCommitLog(topicId);
        List<byte[]> msgList = new ArrayList<>(maxMsgNums);

        while (nums > 0 && index.getIndex(off) != -1) {
            int start = off % SparseSize;
            int end = Math.min(start + nums, SparseSize) - 1;
            try {
                msgList.addAll(commitLog.getMessage((int)index.getIndex(off), start, end));
            } catch (Exception e) {
                e.printStackTrace();
            }

            nums -= (end - start + 1);
            off += (end - start + 1);
        }

        /*直接读缓存*/
        if (nums > 0){
            List<byte[]> cache = queueMsgCache[topicId].readMsgList();
            if (!cache.isEmpty()){
                int start =  off % SparseSize;
                int end = Math.min(cache.size(), start + nums);
                msgList.addAll(cache.subList(start, end));
            }
        }

        return msgList;
    }

    MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
