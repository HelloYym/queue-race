package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/6/24
 * Time: 下午7:34
 */
public interface AppendMessageCallback {
    /**
     * After message serialization, write MapedByteBuffer
     *
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                 final int maxBlank, final byte[] msg);
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                 final int maxBlank, List<byte[]> msg);
}
