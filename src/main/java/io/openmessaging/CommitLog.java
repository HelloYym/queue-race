package io.openmessaging;

import io.openmessaging.common.ServiceThread;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

/**
 * Created by IntelliJ IDEA.
 * User: QWC
 * Date: 2018/6/26
 * Time: 上午9:31
 */
public class CommitLog {

    //采用自旋锁
    private final PutMessageSpinLock putMessageLock;

    private final MappedFileQueue mappedFileQueue;

    private final DefaultMessageStore defaultMessageStore;

    private final FlushCommitLogService flushCommitLogService;

    private final AppendMessageCallback appendMessageCallback;

    //映射文件名
    private static AtomicInteger filePath = new AtomicInteger(0);


    public CommitLog(final DefaultMessageStore defaultMessageStore) {

        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog() + File.separator + filePath.getAndIncrement(),
                defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(), true);

        this.defaultMessageStore = defaultMessageStore;

        //用这种异步刷盘方式
        this.flushCommitLogService = new FlushRealTimeService();

        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());//参数是每个消息最大存储空间

        this.putMessageLock = new PutMessageSpinLock();//采用自旋锁

    }

    /*加载磁盘文件*/
   /* public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }*/


    /*开始写commitlog的同时，应调用这个开启异步刷盘线程*/
    public void start() {
        this.flushCommitLogService.start();
    }


    public void shutdown() {
        this.flushCommitLogService.shutdown();
    }


    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }


    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    //get(offset, 5, 10)的意思是读取当前块中的第5条到第10条记录
//    public ArrayList<byte[]> getMessage(long offset, int start, int end){
//        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
//        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
//
//        if (mappedFile != null) {
//            int SparseSize = defaultMessageStore.getMessageStoreConfig().getSparseSize();
//            int pos = (int) (offset % mappedFileSize);//起始位置
//
//            int[] sizeArray = new int[SparseSize];
//            SelectMappedBufferResult sizeMap;
//            SelectMappedBufferResult message;
//            ArrayList<byte[]> msgList = new ArrayList<>();
//
//            //取出所有消息的大小，放到数组中
//            for(int i=0; i<SparseSize; i++){
//                sizeMap = mappedFile.selectMappedBuffer(pos, 4);
//                byte[] msg = new byte[sizeMap.getSize()];
//                sizeMap.getByteBuffer().get(msg, 0, sizeMap.getSize());
//                sizeArray[i] =byteArrayToInt(msg);
//
//                pos = pos + 4;
//            }
//
//            //此时的pos已经是去掉消息大小部分的位置了
//            for(int i = 0; i < start; i++){
//                pos = pos + sizeArray[i];
//            }
//            //此时pos是start开始要取的位置
//            for(int i = start; i <= end; i++){
//                message = mappedFile.selectMappedBuffer(pos, sizeArray[i]);
//                addMessage(message, msgList);
//                pos += sizeArray[i];
//            }
//
//            return msgList;
//
//        }
//        return null;
//    }
    public ArrayList<byte[]> getMessage(long offset, int start, int end){
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);

        if (mappedFile != null) {
            int SparseSize = defaultMessageStore.getMessageStoreConfig().getSparseSize();
            int pos = (int) (offset % mappedFileSize);//起始位置

            int[] sizeArray = new int[SparseSize];
            SelectMappedBufferResult sizeMap;
            SelectMappedBufferResult message;
            ArrayList<byte[]> msgList = new ArrayList<>();

            int msgPos = pos + 4 * SparseSize;

            //此时的pos已经是去掉消息大小部分的位置了
            for(int i = 0; i < start; i++){

                msgPos = msgPos + mappedFile.selectMappedBuffer(pos, 4).getByteBuffer().getInt();
                pos = pos + 4;

            }
            //此时pos是start开始要取的位置
            for(int i = start; i <= end; i++){

                int size = mappedFile.selectMappedBuffer(pos, 4).getByteBuffer().getInt();
                message = mappedFile.selectMappedBuffer(msgPos, size);
                addMessage(message, msgList);

                msgPos = msgPos + size;
                pos = pos + 4;

            }

            return msgList;
        }
        return null;
    }

    //byte转为int
    public static int byteArrayToInt(byte[] b) {
        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }


    public void addMessage(SelectMappedBufferResult mappedBufferResult, ArrayList<byte[]> msglist){
        byte[] msg = new byte[mappedBufferResult.getSize()];
        mappedBufferResult.getByteBuffer().get(msg, 0, mappedBufferResult.getSize());
        msglist.add(msg);
    }

    //读消息
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }




    //写一串数据
    public PutMessageResult putMessage(List<byte[]> msgs) {
        AppendMessageResult result = null;
        // commitlog中最后一个文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        putMessageLock.lock(); //自旋锁上锁
        try {
            /*如果没有文件或最后一个文件写满，新建一个文件*/
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            /*创建文件失败*/
            if (null == mappedFile) {
                System.out.println("create mapped file1 error" );
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            //回调函数，开始写
            result = mappedFile.appendMessage(msgs, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        System.out.println("create mapped file2 error" );
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessage(msgs, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

        } finally {
            putMessageLock.unlock();
        }

        //每次写消息，都会另一个线程异步刷盘
        flushCommitLogService.wakeup();

        return new PutMessageResult(PutMessageStatus.PUT_OK, result);
    }


    //写消息
    public PutMessageResult putMessage(byte[] msg) {
        // Back to Results
        AppendMessageResult result = null;
        // commitlog中最后一个文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        putMessageLock.lock(); //自旋锁上锁
        try {
            /*如果没有文件或最后一个文件写满，新建一个文件*/
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            /*创建文件失败*/
            if (null == mappedFile) {
                System.out.println("create mapped file1 error" );
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            //回调函数，开始写
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        System.out.println("create mapped file2 error" );
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

        } finally {
            putMessageLock.unlock();
        }

        //每次写消息，都会另一个线程异步刷盘
        flushCommitLogService.wakeup();

        return new PutMessageResult(PutMessageStatus.PUT_OK, result);
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4;//最后留4个字节
        // The maximum length of the message
        private final int maxMessageSize;

        private final int SparseSize = defaultMessageStore.getMessageStoreConfig().getSparseSize();
//        private final ByteBuffer msgStoreSizeMemory;//存的是20个消息的大小
        private final ByteBuffer msgStoreItemMemory;//存的是20个消息的内容


        DefaultAppendMessageCallback(final int size) {
            this.maxMessageSize = size;

//            this.msgStoreSizeMemory = ByteBuffer.allocate(SparseSize * 4);
            //最大是4M，如果连续存20个的话 20*60个字节  足够了
            this.msgStoreItemMemory = ByteBuffer.allocate(0);
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit){
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

        //这个是写一串数据
        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, List<byte[]> msgInner) {

            long wroteOffset = fileFromOffset + byteBuffer.position();

            int bodyLength = 0;

            for(int i = 0; i < msgInner.size(); i++){
                bodyLength = bodyLength + msgInner.get(i).length;
            }

            final int msgLen = bodyLength + SparseSize * 4;//消息长度

            // 如果消息大小大于最大消息限制
//            if (msgLen > this.maxMessageSize) {
//                System.out.println("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
//                        + ", maxMessageSize: " + this.maxMessageSize);
//                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
//            }

            // 如果当前文件没有足够的空间存储这条消息
            if ((msgLen) > maxBlank) {
                byte[] bytes = new byte[maxBlank];
                byteBuffer.put(bytes, 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank);
            }

            for(int j = 0; j < msgInner.size(); j++){
                byteBuffer.putInt(msgInner.get(j).length);
            }
            for(int j = 0; j < msgInner.size(); j++){
                byteBuffer.put(msgInner.get(j));
            }

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen);
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final byte[] msgInner) {

            long wroteOffset = fileFromOffset + byteBuffer.position();
            final int bodyLength = msgInner == null ? 0 : msgInner.length;
            final int msgLen = bodyLength;//消息长度

            // 如果消息大小大于最大消息限制
            if (msgLen > this.maxMessageSize) {
                System.out.println("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // 如果当前文件没有足够的空间存储这条消息
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank);
            }

            // 如果消息可以写到当前文件里 那就写吧
            byteBuffer.put(msgInner, 0, msgLen);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen);
            return result;
        }
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    class FlushRealTimeService extends FlushCommitLogService {
        /**
         * 最后flush时间戳
         */
        private long lastFlushTimestamp = 0;

        @Override
        public void shutdown() {
            this.stopped = true;
        }

        public void run() {

            while (!this.isStopped()) {

                //这些应该可以不要
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                // 控制最小刷盘页数 强制刷盘
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                }

                try {
                    this.waitForRunning(interval);
                    long begin = System.currentTimeMillis();

                    /*刷盘*/
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);

                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        System.out.println("Flush data to disk costs" + past + " ms ");
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
            }

        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }
}
