package org.queue.log;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 日志统计类，实现LogStatsMBean接口。
 */
class LogStats implements LogStatsMBean {
    // 日志对象
    private final Log log;
    // 用于累计追加消息数量的原子长整型变量
    private final AtomicLong numCumulatedMessages;

    public LogStats(Log log) {
        this.log = log;
        this.numCumulatedMessages = new AtomicLong(0);
    }

    @Override
    public String getName() {
        return log.getName(); // 获取日志名称
    }

    @Override
    public long getSize() {
        return log.getSize(); // 获取日志大小
    }

    @Override
    public int getNumberOfSegments() {
        return log.getNumberOfSegments(); // 获取日志分段数量
    }

    @Override
    public long getCurrentOffset() {
        return log.getHighwaterMark(); // 获取当前偏移量
    }

    @Override
    public long getNumAppendedMessages() {
        return numCumulatedMessages.get(); // 获取累计追加的消息数量
    }

    /**
     * 记录追加的消息数量。
     * @param nMessages 追加的消息数量
     */
    public void recordAppendedMessages(int nMessages) {
        numCumulatedMessages.getAndAdd(nMessages); // 累计追加的消息数量
    }
}