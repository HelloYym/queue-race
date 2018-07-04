package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultQueueStoreImpl extends QueueStore {

    private final MessageStoreConfig messageStoreConfig;

    private final DefaultMessageStore messageStore;

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public DefaultQueueStoreImpl() {
        this.messageStoreConfig = new MessageStoreConfig();
        this.messageStore = new DefaultMessageStore(messageStoreConfig);
    }

    @Override
    void put(String queueName, byte[] message) {
        messageStore.putMessage(TopicIdGenerator.getInstance().getId(queueName), message);
    }

    @Override
    Collection<byte[]> get(String queueName, long offset, long num) {
        return messageStore.getMessage(TopicIdGenerator.getInstance().getId(queueName), offset, num);
    }
}
