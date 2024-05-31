package org.queue.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * @author gaoyvfeng
 * @ClassName SimpleConsumerStatsManager
 * @description:
 * @datetime 2024年 05月 24日 09:39
 * @version: 1.0
 */
public class SimpleConsumerStatsManager {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerStatsManager.class);
    private static final String simpleConsumerStatsMBeanName = "queue:type=queue.SimpleConsumerStats";
    private static SimpleConsumerStats stats = new SimpleConsumerStats();

    static {
        try {
            // 注册MBean，这里需要使用Java的JMX（Java Management Extensions）API
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(simpleConsumerStatsMBeanName);
            mbs.registerMBean(stats, objectName);
        } catch (Exception e) {
            logger.error("Error registering MBean", e);
        }
    }

    public static void recordFetchRequest(long requestMs) {
        stats.recordFetchRequest(requestMs);
    }

    public static void recordConsumptionThroughput(long data) {
        stats.recordConsumptionThroughput(data);
    }
}