package org.queue.network;

import org.queue.server.QueueRequestHandlers;
import org.queue.utils.SystemTime;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class SocketServer {
    private  int port; // 服务器端口
    private  int numProcessorThreads; // 处理器线程数量
    private  int monitoringPeriodSecs; // 监控周期（秒）
    private  QueueRequestHandlers handlerFactory; // 处理器工厂，用于创建处理器
    private Acceptor acceptor; // 接受器线程
    private SocketServerStats stats; // 服务器统计信息
    private Processor[] processors; // 处理器数组
    private static final Logger logger = LoggerFactory.getLogger(SocketServer.class.getName());

    public SocketServer(int port, int numProcessorThreads, int monitoringPeriodSecs, QueueRequestHandlers handlerFactory) throws IOException {
        this.port = port;
        this.numProcessorThreads = numProcessorThreads;
        this.monitoringPeriodSecs = monitoringPeriodSecs;
        this.handlerFactory = handlerFactory;
        this.processors = new Processor[numProcessorThreads];
        this.acceptor = new Acceptor(port, processors);
        this.stats = new SocketServerStats(monitoringPeriodSecs);
    }

    /**
     * 启动套接字服务器
     */
    public void startup() throws IOException {
        for (int i = 0; i < numProcessorThreads; i++) {
            processors[i] = new Processor(handlerFactory, SystemTime.getInstance(), stats);
            Utils.newThread("queue-processor-" + i, processors[i], false).start();
        }
        Utils.newThread("queue-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
    }

    /**
     * 关闭套接字服务器
     */
    public void shutdown() {
        acceptor.shutdown();
        for (Processor processor : processors) {
            processor.shutdown();
        }
    }
    public SocketServerStats getStats() {
        return stats;
    }
}