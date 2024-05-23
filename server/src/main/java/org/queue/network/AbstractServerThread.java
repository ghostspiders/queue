package org.queue.network;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.channels.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抽象服务器线程类，为服务器线程提供基本的生命周期管理功能。
 */
public abstract class AbstractServerThread implements Runnable {

    protected Selector selector; // 用于非阻塞I/O操作的选择器
    protected Logger logger; // 日志记录器
    private CountDownLatch startupLatch; // 启动完成信号量
    private CountDownLatch shutdownLatch; // 关闭完成信号量
    private AtomicBoolean alive; // 服务器线程是否存活的标志

    public AbstractServerThread() {
        this.selector = Selector.open(); // 打开选择器
        this.logger = LoggerFactory.getLogger(getClass()); // 获取当前类的日志记录器
        this.startupLatch = new CountDownLatch(1); // 初始化启动信号量为1
        this.shutdownLatch = new CountDownLatch(1); // 初始化关闭信号量为1
        this.alive = new AtomicBoolean(false); // 初始化服务器线程为非存活状态
    }

    /**
     * 优雅地关闭服务器线程，发送停止信号并等待关闭完成。
     */
    public void shutdown() {
        alive.set(false); // 设置存活标志为false
        try {
            selector.wakeup(); // 唤醒选择器
        } catch (Exception e) {
            logger.error("Failed to wakeup selector", e);
        }
        try {
            shutdownLatch.await(); // 等待关闭完成
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
            logger.error("Shutdown interrupted", e);
        }
    }

    /**
     * 等待服务器线程完全启动。
     */
    public void awaitStartup() {
        try {
            startupLatch.await(); // 等待启动完成
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
            logger.error("Startup await interrupted", e);
        }
    }

    /**
     * 标记服务器线程启动完成。
     */
    protected void startupComplete() {
        alive.set(true); // 设置存活标志为true
        startupLatch.countDown(); // 启动信号量减1
    }

    /**
     * 标记服务器线程关闭完成。
     */
    protected void shutdownComplete() {
        shutdownLatch.countDown(); // 关闭信号量减1
    }

    /**
     * 检查服务器线程是否仍在运行。
     * @return 如果服务器线程存活返回true，否则返回false。
     */
    protected boolean isRunning() {
        return alive.get();
    }

    @Override
    public abstract void run(); // 抽象方法，必须由子类实现

    // 在这里可以添加其他需要的抽象方法或具体实现
}