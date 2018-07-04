package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-06-26
 */
public class GetMessageResult {

    private static final double SIZE_PER_COUNT = 64 * 1024;

    private final List<byte[]> messageList = new ArrayList<>(100);

    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;

    private int bufferTotalSize = 0;

    private boolean suggestPullingFromSlave = false;

    public GetMessageResult() {
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        byte[] msg = new byte[mapedBuffer.getSize()];
        mapedBuffer.getByteBuffer().get(msg, 0, mapedBuffer.getSize());
        this.messageList.add(msg);
    }

    public List<byte[]> getMessageList() {
        return messageList;
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public int getMessageCount() {
        return this.messageList.size();
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
                + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
                + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }
}
