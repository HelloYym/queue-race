package io.openmessaging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.config.MessageStoreConfig.MAX_QUEUE_NUM;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-04
 * Time: 下午10:21
 */
public class TopicIdGenerator {

    private static TopicIdGenerator instance = new TopicIdGenerator(MAX_QUEUE_NUM);

    private AtomicInteger id = new AtomicInteger();

    private Map<String, Integer> topicIdMap;

    private TopicIdGenerator(int capacity) {
        topicIdMap = new ConcurrentHashMap<>(capacity);
    }

    public static TopicIdGenerator getInstance() {
        return instance;
    }

    public int getId(String topic) {
        if (!topicIdMap.containsKey(topic)) {
            int topicId = id.getAndIncrement();
            topicIdMap.put(topic, topicId);
            DefaultMessageStore.queueIndexTable[topicId] = new QueueIndex();
            DefaultMessageStore.queueMsgCache[topicId] = new QueueCache();
            return topicId;
        } else {
            return topicIdMap.get(topic);
        }
    }
}
