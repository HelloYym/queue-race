package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    private final CommitLog commitLog;

    private final ConcurrentMap<Integer, QueueIndex> queueIndexTable;

    private final ConcurrentMap<Integer, List<byte[]>> queueMsgCache;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.commitLog = new CommitLog(this);
        this.queueIndexTable = new ConcurrentHashMap<>(MAX_QUEUE_NUM);
        this.queueMsgCache = new ConcurrentHashMap<>(MAX_QUEUE_NUM);
    }

    public void putMessage(int topic, byte[] msg) {
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
//            queueMsgCache.put(topic, new ArrayList<>(SparseSize));
            writeToCommitLog(topic, msgList);
            msgList.clear();
        }
    }

    public List<byte[]> getMessage(int topic, long offset, long maxMsgNums) {
        QueueIndex index = queueIndexTable.get(topic);
        List<byte[]> msgList = new ArrayList<>();
        while (maxMsgNums > 0 && index.getIndex((int) offset) != -1) {
            long fileOffset = index.getIndex((int) offset);
            int start = (int) offset % SparseSize;
            int end = Math.min(start + (int) maxMsgNums, SparseSize) - 1;
            try {
                msgList.addAll(commitLog.getMessage(fileOffset, start, end));
            } catch (Exception e) {
                e.printStackTrace();
            }

            maxMsgNums -= (end - start + 1);
            offset += (end - start + 1);
        }

        List<byte[]> cache = queueMsgCache.get(topic);
        if (maxMsgNums > 0 && !cache.isEmpty()) {
            msgList.addAll(cache.subList(0, Math.min(cache.size(), (int) maxMsgNums - 1)));
        }
        return msgList;
    }

    private void writeToCommitLog(int topic, List<byte[]> msgList) {
        PutMessageResult result = this.commitLog.putMessage(msgList);
        if (result.isOk()) {
            QueueIndex index = queueIndexTable.get(topic);
            if (null == index) {
                QueueIndex newIndex = new QueueIndex(SparseSize);
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
