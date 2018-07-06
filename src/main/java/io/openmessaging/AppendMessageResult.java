package io.openmessaging;

/**
 * Created by IntelliJ IDEA.
 * User: QWC
 * Date: 2018/6/26
 * Time: 上午9:31
 */


/**
 * When write a message to the commit log, returns results
 */
public class AppendMessageResult {
    // Return code
    private AppendMessageStatus status;
    // Where to start writing
    private int wroteOffset;
    // Write Bytes
    private int wroteBytes;


    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0);
    }

    public AppendMessageResult(AppendMessageStatus status, int wroteOffset, int wroteBytes) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
    }


    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public int getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(int wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }


    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", wroteOffset=" + wroteOffset +
                ", wroteBytes=" + wroteBytes +
                '}';
    }
}

