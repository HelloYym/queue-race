package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.config.MessageStoreConfig.MAX_QUEUE_NUM;
import static io.openmessaging.config.MessageStoreConfig.SparseSize;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-06-26
 */
public class DefaultMessageStore {

    private final MessageStoreConfig messageStoreConfig;

    public static final QueueIndex[] queueIndexTable = new QueueIndex[MAX_QUEUE_NUM];

    public static final QueueCache[] queueMsgCache = new QueueCache[MAX_QUEUE_NUM];

    private static final int numCommitLog = 100;

    private final ArrayList<CommitLogLite> commitLogList;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.commitLogList = new ArrayList<>();
        for (int i = 0; i < numCommitLog; i++)
            this.commitLogList.add(new CommitLogLite(Integer.MAX_VALUE, getMessageStoreConfig().getStorePathCommitLog()));
    }

    private CommitLogLite getCommitLog(int topicId){
        return commitLogList.get(topicId % numCommitLog);
    }

    public void putMessage(int topicId, byte[] msg) {
        QueueCache cache = queueMsgCache[topicId];
        cache.addMessage(msg);
        if (cache.size() == SparseSize) {
            writeToCommitLog(topicId, cache.getMsgList());
            cache.clear();
        }
    }

    public List<byte[]> getMessage(int topicId, int offset, int maxMsgNums) {
        int off = offset;
        int nums = maxMsgNums;
        QueueIndex index = queueIndexTable[topicId];
        CommitLogLite commitLog = getCommitLog(topicId);
        List<byte[]> msgList = new ArrayList<>();
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

        List<byte[]> cache = queueMsgCache[topicId].getMsgList();
        if (nums > 0 && !cache.isEmpty()) {
            int start =  off % SparseSize;
            int end = Math.min(cache.size(), start + nums);
            msgList.addAll(cache.subList(start, end));
        }
        return msgList;
    }

    private void writeToCommitLog(int topicId, List<byte[]> msgList) {
        int offset = getCommitLog(topicId).putMessage(msgList);
        if (offset >= 0) {
            queueIndexTable[topicId].putIndex(offset);
        }
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
