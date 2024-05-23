package org.queue.log;

/**
 * 日志统计管理Bean接口，用于提供日志统计信息。
 */
interface LogStatsMBean {
    /**
     * 获取日志名称。
     * @return 日志名称
     */
    String getName();

    /**
     * 获取日志大小。
     * @return 日志大小，单位为字节
     */
    long getSize();

    /**
     * 获取日志分段数量。
     * @return 分段数量
     */
    int getNumberOfSegments();

    /**
     * 获取当前偏移量。
     * @return 当前偏移量
     */
    long getCurrentOffset();

    /**
     * 获取累计追加的消息数量。
     * @return 累计追加的消息数量
     */
    long getNumAppendedMessages();
}