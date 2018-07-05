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
//        System.out.println(topic + "  -------  " + new String(msg));
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

    public List<byte[]> getMessage(int topic, long offset, long maxMsgNums) {
        long off = offset;
        long nums = maxMsgNums;
        QueueIndex index = queueIndexTable.get(topic);
        List<byte[]> msgList = new ArrayList<>();
        while (nums > 0 && index.getIndex((int) off) != -1) {
            long fileOffset = index.getIndex((int) off);
            int start = (int) off % SparseSize;
            int end = Math.min(start + (int) nums, SparseSize) - 1;
            try {
                msgList.addAll(commitLog.getMessage(fileOffset, start, end));
            } catch (Exception e) {
                e.printStackTrace();
            }

            nums -= (end - start + 1);
            off += (end - start + 1);
        }

        List<byte[]> cache = queueMsgCache.get(topic);
        if (nums > 0 && !cache.isEmpty()) {
            int start = (int) off % SparseSize;
            int end = Math.min(cache.size(), start + (int) nums);
            msgList.addAll(cache.subList(start, end));
        }
        return msgList;
    }

    private void writeToCommitLog(int topic, List<byte[]> msgList) {
        PutMessageResult result = this.commitLog.putMessage(msgList);
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
