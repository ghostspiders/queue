package org.queue.network;

/**
 * @author gaoyvfeng
 * @ClassName Transmission
 * @description:
 * @datetime 2024年 05月 22日 10:52
 * @version: 1.0
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抽象类Transmission，用于定义传输操作的共通行为。
 */
public abstract class Transmission {

    // 创建一个日志记录器，用于记录日志信息
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 抽象方法，用于检查传输是否已经完成。
     * 需要在子类中具体实现。
     *
     * @return 如果传输完成返回true，否则返回false。
     */
    public abstract boolean complete();

    /**
     * 保护方法，用于确保在传输未完成时执行操作。
     * 如果传输已经完成，则抛出IllegalStateException异常。
     */
    protected void expectIncomplete() {
        if (complete()) {
            // 如果传输已完成，则抛出异常
            throw new IllegalStateException("This operation cannot be completed on a complete request.");
        }
    }

    /**
     * 保护方法，用于确保在传输完成时执行操作。
     * 如果传输未完成，则抛出IllegalStateException异常。
     */
    protected void expectComplete() {
        if (!complete()) {
            // 如果传输未完成，则抛出异常
            throw new IllegalStateException("This operation cannot be completed on an incomplete request.");
        }
    }
}