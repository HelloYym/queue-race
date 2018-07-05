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

//    private final CommitLog commitLog;

    private final ConcurrentMap<String, List<byte[]>> queueMsgCache;

    private AtomicInteger roundCommitLogCnt = new AtomicInteger(0);
    private static final int numCommitLog = 100;
    private final ArrayList<CommitLog> commitLoglist;
    private final ConcurrentMap<String, Integer> topicCommitLog;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
//        this.commitLog = new CommitLog(this);
        this.queueIndexTable = new ConcurrentHashMap<>(MAX_QUEUE_NUM);
        this.queueMsgCache = new ConcurrentHashMap<>(MAX_QUEUE_NUM);

        this.commitLoglist = new ArrayList<>();
        this.topicCommitLog = new ConcurrentHashMap<>();
        for(int i = 0; i < numCommitLog; i++){
            this.commitLoglist.add(new CommitLog(this));
        }

    }


    private CommitLog getCommitlog(String topic){
        if(!topicCommitLog.containsKey(topic)){
            topicCommitLog.put(topic, roundCommitLogCnt.getAndIncrement() % numCommitLog);
        }
        return commitLoglist.get(topicCommitLog.get(topic));
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
        List<byte[]> msgList = new ArrayList<>();
        while (nums > 0 && index.getIndex(off) != -1) {
            int start = off % SparseSize;
            int end = Math.min(start + nums, SparseSize) - 1;
            try {
                msgList.addAll(getCommitlog(topic).getMessage(index.getIndex(off), start, end));
//                msgList.addAll(commitLog.getMessage(index.getIndex(off), start, end));
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
        PutMessageResult result = getCommitlog(topic).putMessage(msgList);
//        PutMessageResult result = commitLog.putMessage(msgList);
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
