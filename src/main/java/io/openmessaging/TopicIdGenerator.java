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

    private static TopicIdGenerator instance = new TopicIdGenerator();

    private AtomicInteger id = new AtomicInteger();


    private TopicIdGenerator() {
    }

    public static TopicIdGenerator getInstance() {
        return instance;
    }

    public int getId(String topic) {
//        if (!topicIdMap.containsKey(topic)) {
//            int topicId = id.getAndIncrement();
//            topicIdMap.put(topic, topicId);
//            return topicId;
//        } else {
//            return topicIdMap.get(topic);
//        }
        return generateId(topic);
    }

    private int generateId(String topicName) {
        int id = 0;
        int k = 1;
        for (int i = topicName.length() - 1; i >= 0; i--) {
            int num = topicName.charAt(i) - '0';
            if (num >= 0 && num < 10) {
                id += num * k;
                k *= 10;
            } else {
                break;
            }
        }
        return id;
    }
}
