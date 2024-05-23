package org.queue.log;

import org.queue.utils.Range;

import java.io.File;

/**
 * 日志段类，代表日志目录中的一个日志段文件。
 * 每个日志段由一个消息集、起始偏移量和大小组成。
 */
public class LogSegment implements Range {
    // 日志段文件
    private final File file;
    // 消息集，存储日志段中的消息
    private final FileMessageSet messageSet;
    // 日志段的起始偏移量
    private final long start;

    // 标记日志段是否已删除
    private volatile boolean deleted;

    /**
     * 构造函数。
     * @param file 日志段对应的文件。
     * @param messageSet 日志段中的消息集。
     * @param start 日志段的起始偏移量。
     */
    public LogSegment(File file, FileMessageSet messageSet, long start) {
        this.file = file;
        this.messageSet = messageSet;
        this.start = start;
        this.deleted = false; // 初始化时，日志段未被删除
    }

    /**
     * 获取日志段的大小。
     * @return 日志段的大小。
     */
    @Override
    public long size() {
        return messageSet.highWaterMark(); // 假设 FileMessageSet 类有 highWaterMark() 方法
    }

    /**
     * 检查日志段是否已被删除。
     * @return 如果日志段被删除，则返回 true。
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * 设置日志段的删除状态。
     * @param deleted 如果为 true，则标记日志段为已删除。
     */
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    /**
     * 获取日志段的起始偏移量。
     * @return 日志段的起始偏移量。
     */
    @Override
    public long start() {
        return start;
    }

    /**
     * 返回日志段的字符串表示形式。
     * @return 日志段的字符串描述。
     */
    @Override
    public String toString() {
        return "(file=" + file + ", start=" + start + ", size=" + size() + ")";
    }
}