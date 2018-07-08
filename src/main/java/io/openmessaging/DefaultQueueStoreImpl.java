package io.openmessaging;

import io.openmessaging.common.LoggerName;
import io.openmessaging.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class DefaultQueueStoreImpl extends QueueStore {

//    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;

    private final DefaultMessageStore messageStore;

    public DefaultQueueStoreImpl() {
        this.messageStoreConfig = new MessageStoreConfig();
        this.messageStore = new DefaultMessageStore(messageStoreConfig);
    }

    @Override
    void put(String queueName, byte[] message) {
        messageStore.putMessage(TopicIdGenerator.getInstance().getId(queueName), message);
//        messageStore.putMessage(queueName, message);
    }

    @Override
    Collection<byte[]> get(String queueName, long offset, long num) {
//        log.info(String.valueOf(num));
        return messageStore.getMessage(TopicIdGenerator.getInstance().getId(queueName), (int) offset, (int) num);
//        return messageStore.getMessage(queueName, (int) offset, (int) num);
    }
}
