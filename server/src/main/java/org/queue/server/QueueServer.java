package org.queue.server;

/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.queue.log.LogManager;
import org.queue.network.SocketServer;
import org.queue.network.SocketServerStats;
import org.queue.utils.QueueScheduler;
import org.queue.utils.SystemTime;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueServer {
    // 干净关闭文件的名称
    private static final String CLEAN_SHUTDOWN_FILE = ".queue_cleanshutdown";
    // 用于指示是否正在关闭的原子布尔变量
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(QueueServer.class);
    // 关闭时的同步锁
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    // Socket服务器统计信息的MBean名称
    private final String statsMBeanName = "queue:type=queue.SocketServerStats";
    // 日志管理器的引用
    private LogManager logManager;
    // 套接字服务器的引用
    private SocketServer socketServer;
    // 日志清理调度器
    private QueueScheduler scheduler;

    // 服务器配置
    private QueueConfig config;

    public QueueServer(QueueConfig config) {
        this.config = config;
        this.scheduler = new QueueScheduler(1, "queue-logcleaner-", false);
    }

    /**
     * 启动服务器的API。
     * 实例化LogManager、SocketServer和请求处理器-queueRequestHandlers。
     */
    public void startup() {
        try {
            logger.info("Starting queue server...");
            boolean needRecovery = true;
            File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
            if (cleanShutDownFile.exists()) {
                needRecovery = false;
                cleanShutDownFile.delete();
            }
            // 创建日志管理器实例
            logManager = new LogManager(config, scheduler, SystemTime.getInstance(),
                    1000L * 60 * config.getLogCleanupIntervalMinutes(),
                    1000L * 60 * 60 * config.getLogRetentionHours(),
                    needRecovery);
            // 创建请求处理器实例（需要根据实际情况实现RequestHandlers类）
            QueueRequestHandlers handlers = new QueueRequestHandlers(logManager);
            // 创建套接字服务器实例
            socketServer = new SocketServer(config.getPort(), config.getNumThreads(),
                    config.getMonitoringPeriodSecs(), handlers);
            // 注册MBean
            Utils.registerMBean(socketServer.getStats(), statsMBeanName);
            socketServer.startup();
            // 在ZK中注册此代理
            logManager.startup();
            logger.info("Server started.");
        } catch (Throwable e) {
            logger.debug("Error during startup: ", e);
            Utils.stackTrace(e);
            shutdown();
        }
    }

    /**
     * 关闭服务器单个实例的API。
     * 关闭LogManager、SocketServer和日志清理调度线程。
     */
    public void shutdown() {
        if (isShuttingDown.compareAndSet(false, true)) {
            logger.info("Shutting down...");
            try {
                scheduler.shutdown();
                socketServer.shutdown();
                Utils.unregisterMBean(statsMBeanName);
                logManager.close();
                // 创建干净关闭文件
                File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
                if (!cleanShutDownFile.exists()) {
                    cleanShutDownFile.createNewFile();
                }
            } catch (Throwable e) {
                logger.debug("Error during shutdown: ", e);
                Utils.stackTrace(e);
            }
            shutdownLatch.countDown();
            logger.info("Shut down completed");
        }
    }

    /**
     * 在调用shutdown()之后，使用此API等待关闭完成。
     */
    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    // 获取日志管理器的引用
    public LogManager getLogManager() {
        return logManager;
    }

    // 获取Socket服务器统计信息的引用
    public SocketServerStats getStats() {
        return socketServer.getStats();
    }

}