package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.Collection;

public class DefaultQueueStoreImpl extends QueueStore {

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
        return messageStore.getMessage(TopicIdGenerator.getInstance().getId(queueName), (int) offset, (int) num);
//        return messageStore.getMessage(queueName, (int) offset, (int) num);
    }
}
