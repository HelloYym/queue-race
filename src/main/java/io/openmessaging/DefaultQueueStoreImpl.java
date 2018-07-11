package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.Collection;

public class DefaultQueueStoreImpl extends QueueStore {

    private final MessageStoreConfig messageStoreConfig;

    private final DefaultMessageStore messageStore;

    private TopicIdGenerator idGenerator = new TopicIdGenerator();

    public DefaultQueueStoreImpl() {
        this.messageStoreConfig = new MessageStoreConfig();
        this.messageStore = new DefaultMessageStore(messageStoreConfig);
    }

    @Override
    void put(String queueName, byte[] message) {
        int queueId = idGenerator.getId(queueName);
        messageStore.putMessage(queueId, message);
    }

    @Override
    Collection<byte[]> get(String queueName, long offset, long num) {
        int queueId = idGenerator.getId(queueName);
        return messageStore.getMessage(queueId, (int) offset, (int) num);
    }
}
