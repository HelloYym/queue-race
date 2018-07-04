package io.openmessaging;

import io.openmessaging.common.LoggerName;
import io.openmessaging.common.UtilAll;
import io.openmessaging.config.FlushDiskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by IntelliJ IDEA.
 * User: QWC
 * Date: 2018/6/26
 * Time: 上午9:31
 */
public class MappedFile extends ReferenceResource {

    public static final int OS_PAGE_SIZE = 1024 * 4;//默认页大小为4k
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);//JVM中映射的虚拟内存总大小
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);//JVM中mmap的数量
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);//当前写文件的位置

    /*记录当前文件刷盘刷到哪个位置*/
//    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    private final AtomicInteger flushedPosition = new AtomicInteger(0);


    protected int fileSize;//映射文件的大小
    protected FileChannel fileChannel;//映射的fileChannel对象

    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    /*目前不使用writeBuffer*/
//    protected ByteBuffer writeBuffer = null;

//    protected TransientStorePool transientStorePool = null;

    private String fileName;//映射的文件名
    private long fileFromOffset;//映射的起始偏移量，等于文件名
    private File file;//映射的文件
    private MappedByteBuffer mappedByteBuffer;//映射的内存对象

    private boolean firstCreateInQueue = false;//是不是刚刚创建的

    private boolean isMapped;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize, final boolean isMapped) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());//起始偏移量就是文件名
        boolean ok = false;

        this.isMapped = isMapped;

        /*检查文件夹是否存在*/
        ensureDirOK(this.file.getParent());

        /*打开文件，并将文件映射到内存*/
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            if (isMapped)
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);//JVM中映射的虚拟内存总大小
            TOTAL_MAPPED_FILES.incrementAndGet();//JVM中mmap的数量
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }


    /*commitlog使用这种写文件方式,暂时用这种回调的方式，后面直接把函数写在这里不用回调*/
    public AppendMessageResult appendMessage(final byte[] message, final AppendMessageCallback cb) {
        assert message != null;
        assert cb != null;

        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {
            //不使用writebuff，使用mappedByteBuffer
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = null;
            result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, message);
            //写完之后，将写指针移到写的最后
            this.wrotePosition.addAndGet(result.getWroteBytes());
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }


    public AppendMessageResult appendMessage(final List<byte[]> message, final AppendMessageCallback cb) {
        assert message != null;
        assert cb != null;

        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {
            //不使用writebuff，使用mappedByteBuffer
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = null;
            result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, message);
            //写完之后，将写指针移到写的最后
            this.wrotePosition.addAndGet(result.getWroteBytes());
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }


    /*直接写入file channel，consumqueue用这种方式*/
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            //写完之后，将写指针移到写的最后
            this.wrotePosition.addAndGet(data.length);
            return true;
        }
        return false;
    }

    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }


    /*使用这种方式刷盘，不要用commit*/
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    if (this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }

                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.wrotePosition.get();
    }


    /*读取指定位置指定大小的buffer，用这个*/
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                if (this.isMapped) {
                    ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                    byteBuffer.position(pos);
                    ByteBuffer byteBufferNew = byteBuffer.slice();
                    byteBufferNew.limit(size);
                    return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
                } else {
                    try {
                        ByteBuffer byteBufferNew = ByteBuffer.allocate(size);
                        this.fileChannel.position(pos);
                        this.fileChannel.read(byteBufferNew);
                        return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }
        return null;
    }

    /*读取指定位置至结尾的buffer*/
    public SelectMappedBufferResult selectMappedBuffer(int pos) {

        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                if (this.isMapped){
                    ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                    byteBuffer.position(pos);
                    int size = readPosition - pos;
                    ByteBuffer byteBufferNew = byteBuffer.slice();
                    byteBufferNew.limit(size);

                    return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
                }
                else{
                    try {
                        int size = readPosition - pos;
                        ByteBuffer byteBufferNew = ByteBuffer.allocate(size);
                        this.fileChannel.read(byteBufferNew, pos);
                        byteBufferNew.flip();
                        return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return null;
    }


    //清楚buffer缓存操作
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }


    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

//    锁定物理内存，阻止swap
//    public void mlock() {
//        final long beginTime = System.currentTimeMillis();
//        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
//        Pointer pointer = new Pointer(address);
//        {
//            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
//            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
//        }
//
//        {
//            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
//            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
//        }
//    }
//
//    public void munlock() {
//        final long beginTime = System.currentTimeMillis();
//        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
//        Pointer pointer = new Pointer(address);
//        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
//        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
//    }

    //testable

}
