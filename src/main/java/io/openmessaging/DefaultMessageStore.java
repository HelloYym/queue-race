package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.config.MessageStoreConfig.SparseSize;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-06-26
 */
public class DefaultMessageStore {

    private static final int MAX_QUEUE_NUM = 1100000;

    private final MessageStoreConfig messageStoreConfig;

    private final ConcurrentMap<String, QueueIndex> queueIndexTable;

    private final ConcurrentMap<String, List<byte[]>> queueMsgCache;

    private static final int COMMITLOG_NUM = 50;

    private final ArrayList<CommitLog> commitLoglist;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.queueIndexTable = new ConcurrentHashMap<>(MAX_QUEUE_NUM);
        this.queueMsgCache = new ConcurrentHashMap<>(MAX_QUEUE_NUM);

        this.commitLoglist = new ArrayList<>(COMMITLOG_NUM);
        for(int i = 0; i < COMMITLOG_NUM; i++){
            this.commitLoglist.add(new CommitLog(this));
        }
    }

    private CommitLog getCommitLog(String topic){
        return commitLoglist.get(Math.abs(topic.hashCode()) % COMMITLOG_NUM);
    }

    public void putMessage(String topic, byte[] msg) {
        List<byte[]> msgList = queueMsgCache.get(topic);
        if (msgList == null) {
            List<byte[]> newList = new ArrayList<>(SparseSize);
            List<byte[]> old = queueMsgCache.putIfAbsent(topic, newList);
            if (old != null) {
                msgList = old;
            } else {
                msgList = newList;
            }
        }
        msgList.add(msg);
        if (msgList.size() == SparseSize) {
            writeToCommitLog(topic, msgList);
            msgList.clear();
        }
    }

    public List<byte[]> getMessage(String topic, int offset, int maxMsgNums) {
        int off = offset;
        int nums = maxMsgNums;
        QueueIndex index = queueIndexTable.get(topic);
        CommitLog commitLog = getCommitLog(topic);
        List<byte[]> msgList = new ArrayList<>();
        while (nums > 0 && index.getIndex(off) != -1) {
            int start = off % SparseSize;
            int end = Math.min(start + nums, SparseSize) - 1;
            try {
                msgList.addAll(commitLog.getMessage(index.getIndex(off), start, end));
            } catch (Exception e) {
                e.printStackTrace();
            }

            nums -= (end - start + 1);
            off += (end - start + 1);
        }

        List<byte[]> cache = queueMsgCache.get(topic);
        if (nums > 0 && !cache.isEmpty()) {
            int start =  off % SparseSize;
            int end = Math.min(cache.size(), start + nums);
            msgList.addAll(cache.subList(start, end));
        }
        return msgList;
    }

    private void writeToCommitLog(String topic, List<byte[]> msgList) {
        PutMessageResult result = getCommitLog(topic).putMessage(msgList);
        if (result.isOk()) {
            QueueIndex index = queueIndexTable.get(topic);
            if (null == index) {
                QueueIndex newIndex = new QueueIndex();
                QueueIndex oldIndex = queueIndexTable.putIfAbsent(topic, newIndex);
                if (oldIndex != null) {
                    index = newIndex;
                } else {
                    index = newIndex;
                }
            }
            index.putIndex(result.getAppendMessageResult().getWroteOffset());
        }
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
