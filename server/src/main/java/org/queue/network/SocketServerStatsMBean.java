package org.queue.network;

/**
 * 服务器统计信息的Java接口，用于获取服务器的统计数据。
 */
public interface SocketServerStatsMBean {

    /**
     * 获取每秒生产请求的数量。
     * @return 每秒生产请求的数量，以双精度浮点数形式返回。
     */
    double getProduceRequestsPerSecond();

    /**
     * 获取每秒获取请求的数量。
     * @return 每秒获取请求的数量，以双精度浮点数形式返回。
     */
    double getFetchRequestsPerSecond();

    /**
     * 获取生产请求的平均处理时间（毫秒）。
     * @return 生产请求的平均处理时间，以双精度浮点数形式返回。
     */
    double getAvgProduceRequestMs();

    /**
     * 获取生产请求的最大处理时间（毫秒）。
     * @return 生产请求的最大处理时间，以双精度浮点数形式返回。
     */
    double getMaxProduceRequestMs();

    /**
     * 获取获取请求的平均处理时间（毫秒）。
     * @return 获取请求的平均处理时间，以双精度浮点数形式返回。
     */
    double getAvgFetchRequestMs();

    /**
     * 获取获取请求的最大处理时间（毫秒）。
     * @return 获取请求的最大处理时间，以双精度浮点数形式返回。
     */
    double getMaxFetchRequestMs();

    /**
     * 获取每秒读取的字节数。
     * @return 每秒读取的字节数，以双精度浮点数形式返回。
     */
    double getBytesReadPerSecond();

    /**
     * 获取每秒写入的字节数。
     * @return 每秒写入的字节数，以双精度浮点数形式返回。
     */
    double getBytesWrittenPerSecond();

    /**
     * 获取获取请求的总数。
     * @return 获取请求的总数，以长整型形式返回。
     */
    long getNumFetchRequests();

    /**
     * 获取生产请求的总数。
     * @return 生产请求的总数，以长整型形式返回。
     */
    long getNumProduceRequests();
}