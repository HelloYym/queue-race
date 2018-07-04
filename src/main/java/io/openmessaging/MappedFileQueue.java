package io.openmessaging;

import io.openmessaging.common.LoggerName;
import io.openmessaging.common.UtilAll;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: QWC
 * Date: 2018/6/26
 * Time: 上午9:31
 */

public class MappedFileQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;

    /*文件存储位置*/
    private final String storePath;

    /*每个文件的大小*/
    private final int mappedFileSize;

    /*MapedFile的集合，代表着各个文件*/
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /*当前队列已经存盘的位置*/
    private long flushedWhere = 0;
//    private long committedWhere = 0;

    private boolean isMapped;

    public MappedFileQueue(final String storePath, int mappedFileSize, boolean isMapped) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.isMapped = isMapped;
    }

    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile mappedFile = this.getFirstMappedFile();
            if (mappedFile != null) {
                int index = (int) ((offset / this.mappedFileSize) - (mappedFile.getFileFromOffset() / this.mappedFileSize));
                if (index < 0 || index >= this.mappedFiles.size()) {
                    LOG_ERROR.warn("Offset for {} not matched. Request offset: {}, index: {}, " +
                                    "mappedFileSize: {}, mappedFiles count: {}",
                            mappedFile,
                            offset,
                            index,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                }

                try {
                    return this.mappedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mappedFile;
                    }
                    LOG_ERROR.warn("findMappedFileByOffset failure. ", e);
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
        }
        return result;
    }

    /*从磁盘位置读取已经存在的mappedFile*/
/*    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize, isMapped);
                    *//*已存在的文件都已经写满，所以指针移动到末尾*//*
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
//                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }*/

    /*新建一个mappedFile，并指定该文件起始偏移*/
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {

        MappedFile mappedFileLast = getLastMappedFile();

        /*createOffset是新建文件的起始位置在整个队列中的偏移，等于文件名*/
        long createOffset = -1;

        /*队列中没有文件，createOffset=startOffset*/
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        /*队列中最后一个文件写满了，createOffset=新文件头的偏移*/
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);

            /*直接建立一个新文件*/
            MappedFile mappedFile = null;
            try {
                mappedFile = new MappedFile(nextFilePath, this.mappedFileSize, this.isMapped);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }

            /*队列头*/
            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /*直接取最后一个文件，没有则返回null*/
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }



    /*队列头的offset*/
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /*最后的读指针offset，writeBuffer未使用时，与写指针相同*/
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /*最后的写指针offset*/
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }


    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }





//    /*检查队列中的每个文件offset*/
//    public void checkSelf() {
//
//        if (!this.mappedFiles.isEmpty()) {
//            Iterator<MappedFile> iterator = mappedFiles.iterator();
//            MappedFile pre = null;
//            while (iterator.hasNext()) {
//                MappedFile cur = iterator.next();
//
//                if (pre != null) {
//                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
//                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
//                                pre.getFileName(), cur.getFileName());
//                    }
//                }
//                pre = cur;
//            }
//        }
//    }

//    /*找到在timestamp之后修改的第一个mappedFile*/
//    public MappedFile getMappedFileByTime(final long timestamp) {
//        Object[] mfs = this.copyMappedFiles(0);
//
//        if (null == mfs)
//            return null;
//
//        for (int i = 0; i < mfs.length; i++) {
//            MappedFile mappedFile = (MappedFile) mfs[i];
//            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
//                return mappedFile;
//            }
//        }
//
//        return (MappedFile) mfs[mfs.length - 1];
//    }
//
//    private Object[] copyMappedFiles(final int reservedMappedFiles) {
//        Object[] mfs;
//
//        if (this.mappedFiles.size() <= reservedMappedFiles) {
//            return null;
//        }
//
//        mfs = this.mappedFiles.toArray();
//        return mfs;
//    }
//
//    /*从完整队列里截取前offset字节，丢弃后面的文件，并移动pos指针*/
//    public void truncateDirtyFiles(long offset) {
//        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
//        for (MappedFile file : this.mappedFiles) {
//            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
//            if (fileTailOffset > offset) {
//
//                if (offset >= file.getFileFromOffset()) {   //刚好处于截断位置的文件
//                    file.setWrotePosition((int) (offset % this.mappedFileSize));
//                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
//                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
//                } else {    //截断位置之后的文件直接丢弃
//                    file.destroy(1000);
//                    willRemoveFiles.add(file);
//                }
//            }
//        }
//        this.deleteExpiredFile(willRemoveFiles);
//    }
//
//    /*从mappedFiles列表中删除一些文件*/
//    void deleteExpiredFile(List<MappedFile> files) {
//
//        if (!files.isEmpty()) {
//
//            Iterator<MappedFile> iterator = files.iterator();
//            while (iterator.hasNext()) {
//                MappedFile cur = iterator.next();
//                if (!this.mappedFiles.contains(cur)) {
//                    iterator.remove();
//                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
//                }
//            }
//
//            try {
//                if (!this.mappedFiles.removeAll(files)) {
//                    log.error("deleteExpiredFile remove failed.");
//                }
//            } catch (Exception e) {
//                log.error("deleteExpiredFile has exception.", e);
//            }
//        }
//    }



//    /*还有多少字节数据没有存盘*/
//    public long howMuchFallBehind() {
//        if (this.mappedFiles.isEmpty())
//            return 0;
//
//        long committed = this.flushedWhere;
//        if (committed != 0) {
//            MappedFile mappedFile = this.getLastMappedFile(0, false);
//            if (mappedFile != null) {
//                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
//            }
//        }
//
//        return 0;
//    }


//    /*直接设置队列的当前offset，后面多余的删除*/
//    public boolean resetOffset(long offset) {
//        MappedFile mappedFileLast = getLastMappedFile();
//
//        if (mappedFileLast != null) {
//            /*最后一个文件写指针的位置*/
//            long lastOffset = mappedFileLast.getFileFromOffset() +
//                    mappedFileLast.getWrotePosition();
//            long diff = lastOffset - offset;
//
//            final int maxDiff = this.mappedFileSize * 2;
//            if (diff > maxDiff)
//                return false;
//        }
//
//        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();
//
//        while (iterator.hasPrevious()) {
//            mappedFileLast = iterator.previous();
//            if (offset >= mappedFileLast.getFileFromOffset()) {
//                int where = (int) (offset % mappedFileLast.getFileSize());
//                mappedFileLast.setFlushedPosition(where);
//                mappedFileLast.setWrotePosition(where);
//                mappedFileLast.setCommittedPosition(where);
//                break;
//            } else {
//                iterator.remove();
//            }
//        }
//        return true;
//    }

//    public long remainHowManyDataToCommit() {
//        return getMaxWrotePosition() - committedWhere;
//    }



    /*删除最后一个mappedFile*/
//    public void deleteLastMappedFile() {
//        MappedFile lastMappedFile = getLastMappedFile();
//        if (lastMappedFile != null) {
//            lastMappedFile.destroy(1000);
//            this.mappedFiles.remove(lastMappedFile);
//            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());
//        }
//    }

//    public int deleteExpiredFileByTime(final long expiredTime,
//                                       final int deleteFilesInterval,
//                                       final long intervalForcibly,
//                                       final boolean cleanImmediately) {
//        Object[] mfs = this.copyMappedFiles(0);
//
//        if (null == mfs)
//            return 0;
//
//        int mfsLength = mfs.length - 1;
//        int deleteCount = 0;
//        List<MappedFile> files = new ArrayList<MappedFile>();
//        if (null != mfs) {
//            for (int i = 0; i < mfsLength; i++) {
//                MappedFile mappedFile = (MappedFile) mfs[i];
//                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
//                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
//                    if (mappedFile.destroy(intervalForcibly)) {
//                        files.add(mappedFile);
//                        deleteCount++;
//
//                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
//                            break;
//                        }
//
//                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
//                            try {
//                                Thread.sleep(deleteFilesInterval);
//                            } catch (InterruptedException e) {
//                            }
//                        }
//                    } else {
//                        break;
//                    }
//                } else {
//                    //avoid deleting files in the middle
//                    break;
//                }
//            }
//        }
//
//        deleteExpiredFile(files);
//
//        return deleteCount;
//    }

//    /*没看懂*/
//    public int deleteExpiredFileByOffset(long offset, int unitSize) {
//        Object[] mfs = this.copyMappedFiles(0);
//
//        List<MappedFile> files = new ArrayList<MappedFile>();
//        int deleteCount = 0;
//        if (null != mfs) {
//
//            int mfsLength = mfs.length - 1;
//
//            for (int i = 0; i < mfsLength; i++) {
//                boolean destroy;
//                MappedFile mappedFile = (MappedFile) mfs[i];
//                /*文件末尾unitSize长度的数据*/
//                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
//                if (result != null) {
//                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
//                    result.release();
//                    destroy = maxOffsetInLogicQueue < offset;
//                    if (destroy) {
//                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
//                                + maxOffsetInLogicQueue + ", delete it");
//                    }
//                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
//                    log.warn("Found a hanged consume queue file, attempting to delete it.");
//                    destroy = true;
//                } else {
//                    log.warn("this being not executed forever.");
//                    break;
//                }
//
//                if (destroy && mappedFile.destroy(1000 * 60)) {
//                    files.add(mappedFile);
//                    deleteCount++;
//                } else {
//                    break;
//                }
//            }
//        }
//
//        deleteExpiredFile(files);
//
//        return deleteCount;
//    }



//    public boolean commit(final int commitLeastPages) {
//        boolean result = true;
//        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
//        if (mappedFile != null) {
//            int offset = mappedFile.commit(commitLeastPages);
//            long where = mappedFile.getFileFromOffset() + offset;
//            result = where == this.committedWhere;
//            this.committedWhere = where;
//        }
//        return result;
//    }


//    public long getMappedMemorySize() {
//        long size = 0;
//
//        Object[] mfs = this.copyMappedFiles(0);
//        if (mfs != null) {
//            for (Object mf : mfs) {
//                if (((ReferenceResource) mf).isAvailable()) {
//                    size += this.mappedFileSize;
//                }
//            }
//        }
//        return size;
//    }

//    /*重新强制删除第一个文件*/
//    public boolean retryDeleteFirstFile(final long intervalForcibly) {
//        MappedFile mappedFile = this.getFirstMappedFile();
//        if (mappedFile != null) {
//            if (!mappedFile.isAvailable()) {
//                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
//                boolean result = mappedFile.destroy(intervalForcibly);
//                if (result) {
//                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
//                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
//                    tmpFiles.add(mappedFile);
//                    this.deleteExpiredFile(tmpFiles);
//                } else {
//                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
//                }
//
//                return result;
//            }
//        }
//
//        return false;
//    }

//    public void shutdown(final long intervalForcibly) {
//        for (MappedFile mf : this.mappedFiles) {
//            mf.shutdown(intervalForcibly);
//        }
//    }
//
//    public void destroy() {
//        for (MappedFile mf : this.mappedFiles) {
//            mf.destroy(1000 * 3);
//        }
//        this.mappedFiles.clear();
//        this.flushedWhere = 0;
//
//        // delete parent directory
//        File file = new File(storePath);
//        if (file.isDirectory()) {
//            file.delete();
//        }
//    }



//    public long getCommittedWhere() {
//        return committedWhere;
//    }
//
//    public void setCommittedWhere(final long committedWhere) {
//        this.committedWhere = committedWhere;
//    }
}
