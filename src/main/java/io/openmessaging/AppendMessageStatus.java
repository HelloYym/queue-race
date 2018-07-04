package io.openmessaging;

/**
 * Created by IntelliJ IDEA.
 * User: QWC
 * Date: 2018/6/26
 * Time: 上午9:31
 */

/**
 * When write a message to the commit log, returns code
 */
public enum AppendMessageStatus {
    PUT_OK,
    END_OF_FILE,
    MESSAGE_SIZE_EXCEEDED,
    PROPERTIES_SIZE_EXCEEDED,
    UNKNOWN_ERROR,
}
