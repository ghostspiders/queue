package org.queue.network;

public interface ConnectionConfig {
    // 定义接口方法，实现类需要提供主机地址
    String getHost();
    // 定义接口方法，实现类需要提供端口号
    int getPort();
    // 发送缓冲区大小，默认实现返回-1，表示使用系统默认值
    default int getSendBufferSize() { return -1; }
    // 接收缓冲区大小，默认实现返回-1，表示使用系统默认值
    default int getReceiveBufferSize() { return -1; }
    // TCP立即发送数据标志，默认为true
    default boolean isTcpNoDelay() { return true; }
    // TCP保活机制标志，默认为false
    default boolean isKeepAlive() { return false; }
}