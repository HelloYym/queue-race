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

        int queueId = Integer.parseInt(queueName.substring(6));
        messageStore.putMessage(queueId, message);

//        messageStore.putMessage(TopicIdGenerator.getInstance().getId(queueName), message);
    }

    @Override
    Collection<byte[]> get(String queueName, long offset, long num) {
        int queueId = Integer.parseInt(queueName.substring(6));
        return messageStore.getMessage(queueId, (int) offset, (int) num);

//        return messageStore.getMessage(TopicIdGenerator.getInstance().getId(queueName), (int) offset, (int) num);
    }
}
