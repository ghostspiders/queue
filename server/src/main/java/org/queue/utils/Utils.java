package org.queue.utils;
import org.queue.message.CompressionCodec;
import org.queue.message.NoCompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.*;
import java.lang.management.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.CRC32;
import javax.management.*;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * 将给定的函数封装为java.lang.Runnable
     * @param fun 一个无参函数
     * @return 一个Runnable，执行该函数
     */
    public static Runnable runnable(Runnable fun) {
        return new Runnable() {
            @Override
            public void run() {
                fun.run();
            }
        };
    }

    /**
     * 将给定的函数封装为java.lang.Runnable，并记录任何遇到的错误
     * @param fun 一个无参函数
     * @return 一个Runnable，执行该函数，并记录错误
     */
    public static Runnable loggedRunnable(Runnable fun) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    fun.run();
                } catch (Throwable t) {
                    // 记录任何错误和堆栈跟踪
                    logger.error(t.getMessage(), t);
                    logger.error(stackTrace(t));
                }
            }
        };
    }

    /**
     * 创建一个守护线程
     * @param name 线程名称
     * @param runnable 线程要执行的Runnable
     * @return 未启动的线程
     */
    public static Thread daemonThread(String name, Runnable runnable) {
        return newThread(name, runnable, true);
    }

    /**
     * 创建一个新线程
     * @param name 线程名称
     * @param runnable 线程要执行的Runnable
     * @param daemon 是否作为守护线程运行
     * @return 未启动的线程
     */
    public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        return thread;
    }


    /**
     * 从给定缓冲区的指定偏移量和大小中读取字节数组
     */
    public static byte[] readBytes(ByteBuffer buffer, int offset, int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = buffer.get(offset + i);
        }
        return bytes;
    }

    /**
     * 读取一个由2字节短整型大小前缀的字符串
     */
    public static String readShortString(ByteBuffer buffer,Charset encoding) {
        short size = buffer.getShort();
        if (size < 0) {
            return null;
        }
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new String(bytes, encoding);
    }

    /**
     * 写入一个由2字节短整型大小前缀的字符串
     */
    public static void writeShortString(ByteBuffer buffer, String string, Charset encoding) {
        if (string == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = string.getBytes(encoding);
            if (bytes.length > Short.MAX_VALUE) {
                throw new IllegalArgumentException("String exceeds maximum size of " + Short.MAX_VALUE + ".");
            }
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }
    }

    /**
     * 加载属性文件
     */
    public static Properties loadProps(String filename) throws IOException {
        Properties props = new Properties();
        try (InputStream propStream = new FileInputStream(filename)) {
            props.load(propStream);
        }
        return props;
    }

    /**
     * 读取必需的整数值属性
     */
    public static int getInt(Properties props, String name) {
        if (props.containsKey(name)) {
            return getInt(props, name, -1);
        } else {
            throw new IllegalArgumentException("Missing required property '" + name + "'");
        }
    }

    /**
     * 读取整数值属性，如果未找到则使用默认值
     */
    public static int getInt(Properties props, String name, int defaultValue) {
        String value = props.getProperty(name);
        return (value != null) ? Integer.parseInt(value) : defaultValue;
    }

    /**
     * 从属性实例中读取一个整数。如果该值不在给定范围内（包含），则抛出异常。
     * @param props 要读取的属性对象
     * @param name 属性名称
     * @param range 值必须落入的范围内界（包含）
     * @return 整数值
     * @throws IllegalArgumentException 如果值不在给定范围内
     */
    public static int getIntInRange(Properties props, String name, int defaultVal, int[] range) {
        int v;
        try {
            if (props.containsKey(name)) {
                v = Integer.parseInt(props.getProperty(name));
            } else {
                v = defaultVal;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Property '" + name + "' is not a valid integer.");
        }

        if (v < range[0] || v > range[1]) {
            throw new IllegalArgumentException(name + " has value " + v + " which is not in the range " +
                    "[" + range[0] + ", " + range[1] + "].");
        }
        return v;
    }

    /**
     * 从属性实例中读取布尔值。
     * @param props 要读取的属性对象
     * @param name 属性名称
     * @param defaultVal 默认值，如果属性未找到则使用
     * @return 布尔值
     */
    public static boolean getBoolean(Properties props, String name, boolean defaultVal) {
        if (!props.containsKey(name)) {
            return defaultVal;
        }
        String value = props.getProperty(name);
        return "true".equalsIgnoreCase(value);
    }

    /**
     * 获取字符串属性，如果未定义则返回给定的默认值。
     * @param props 要读取的属性对象
     * @param name 属性名称
     * @param defaultVal 默认值，如果属性未找到则使用
     * @return 字符串属性值
     */
    public static String getString(Properties props, String name, String defaultVal) {
        return props.containsKey(name) ? props.getProperty(name) : defaultVal;
    }

    /**
     * 获取字符串属性，如果未定义则抛出异常。
     * @param props 要读取的属性对象
     * @param name 属性名称
     * @return 字符串属性值
     * @throws IllegalArgumentException 如果属性未定义
     */
    public static String getString(Properties props, String name) {
        if (props.containsKey(name)) {
            return props.getProperty(name);
        } else {
            throw new IllegalArgumentException("Missing required property '" + name + "'");
        }
    }

    /**
     * 获取类型为java.util.Properties的属性，如果未定义则抛出异常。
     * @param props 要读取的属性对象
     * @param name 属性名称
     * @return java.util.Properties对象
     * @throws IllegalArgumentException 如果属性未定义或格式不正确
     */
    public static Properties getProps(Properties props, String name) {
        if (props.containsKey(name)) {
            String propString = props.getProperty(name);
            Properties properties = new Properties();
            String[] propValues = propString.split(",");
            for (String propValue : propValues) {
                String[] prop = propValue.split("=");
                if (prop.length != 2) {
                    throw new IllegalArgumentException("Illegal format of specifying properties '" + propValue + "'");
                }
                properties.put(prop[0], prop[1]);
            }
            return properties;
        } else {
            throw new IllegalArgumentException("Missing required property '" + name + "'");
        }
    }

    /**
     * 获取属性的java.util.Properties实例，如果未定义则返回默认值。
     * @param props 要读取的属性对象
     * @param name 属性名称
     * @param defaultProps 默认的属性对象，如果指定属性未找到则使用
     * @return java.util.Properties实例
     */
    public static Properties getProps(Properties props, String name, Properties defaultProps) {
        if (props.containsKey(name)) {
            String propString = props.getProperty(name);
            String[] propValues = propString.split(",");
            if (propValues.length < 1) {
                throw new IllegalArgumentException("Illegal format of specifying properties '" + propString + "'");
            }
            Properties properties = new Properties();
            for (String propValue : propValues) {
                String[] prop = propValue.split("=");
                if (prop.length != 2) {
                    throw new IllegalArgumentException("Illegal format of specifying properties '" + propValue + "'");
                }
                properties.put(prop[0], prop[1]);
            }
            return properties;
        } else {
            return defaultProps;
        }
    }
    /**
     * 打开给定文件的通道
     */
    public static FileChannel openChannel(File file, boolean mutable) throws IOException {
        return mutable ? new RandomAccessFile(file, "rw").getChannel() : new FileInputStream(file).getChannel();
    }

    /**
     * 执行给定操作并吞下（不抛出）任何异常，但记录它们
     */
    public static void swallow(Level logLevel, Throwable e) {
        switch (logLevel) {
            case ERROR:
                logger.error(e.getMessage(), e);
                break;
            case WARN:
                logger.warn(e.getMessage(), e);
                break;
            case INFO:
                logger.info(e.getMessage(), e);
                break;
            case DEBUG:
                logger.debug(e.getMessage(), e);
                break;
            case TRACE:
                logger.trace(e.getMessage(), e);
                break;
            default:
                break;
        }
    }
    // ByteBuffer比较方法
    public static boolean equal(ByteBuffer b1, ByteBuffer b2) {
        if (b1.position() != b2.position() || b1.remaining() != b2.remaining()) {
            return false;
        }
        for (int i = 0; i < b1.remaining(); i++) {
            if (b1.get(i) != b2.get(i)) {
                return false;
            }
        }
        return true;
    }

    // ByteBuffer转换为字符串的方法
    public static String toString(ByteBuffer buffer, String encoding) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes); // 使用duplicate避免改变原始buffer的位置
        try {
            return new String(bytes, encoding);
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported encoding: " + encoding, e);
            return null;
        }
    }

    // 打印错误消息并退出JVM的方法
    public static void croak(String message) {
        System.err.println(message);
        System.exit(1);
    }

    // 递归删除文件或目录的方法
    public static void rm(String file) {
        rm(new File(file));
    }

    public static void rm(File file) {
        if (file == null) return;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) for (File f : files) {
                rm(f);
            }
        }
        file.delete();
    }

    // 注册MBean的方法
    public static void registerMBean(Object mbean, String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objName = new ObjectName(name);
            if (mbs.isRegistered(objName)) {
                mbs.unregisterMBean(objName);
            }
            mbs.registerMBean(mbean, objName);
        } catch (Exception e) {
            logger.error("Error registering MBean: " + name, e);
        }
    }

    // 注销MBean的方法
    public static void unregisterMBean(String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objName = new ObjectName(name);
            if (mbs.isRegistered(objName)) {
                mbs.unregisterMBean(objName);
            }
        } catch (Exception e) {
            logger.error("Error unregistering MBean: " + name, e);
        }
    }

    // 从ByteBuffer中读取无符号整数的方法
    public static long getUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    // 将无符号整数写入ByteBuffer的方法
    public static void putUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int)(value & 0xffffffffL));
    }

    // 计算字节数组的CRC32校验码的方法
    public static long crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }

    // 计算字节数组片段的CRC32校验码的方法
    public static long crc32(byte[] bytes, int offset, int size) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    // 计算多个对象的哈希码的方法
    public static int hashcode(Object... as) {
        if (as == null) return 0;
        int h = 1;
        for (Object a : as) {
            if (a != null) {
                h = 31 * h + a.hashCode();
            }
        }
        return h;
    }

    // 从通道读取字节到缓冲区的方法
    public static int read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
            throw new EOFException("Received -1 when reading from channel, socket has likely been closed.");
        }
        return bytesRead;
    }

    // 确保对象不为null的方法
    public static <V> V notNull(V v) {
        if (v == null) {
            throw new IllegalArgumentException("Value cannot be null.");
        }
        return v;
    }

    // 从字符串中解析主机和端口的方法
    public static Map<String, Integer> getHostPort(String hostport) {
        String[] splits = hostport.split(":");
        Map<String, Integer> map = new HashMap<>();
        map.put(splits[0], Integer.parseInt(splits[1]));
        return map;
    }

    // 从字符串中解析主题和分区的方法
    public static Map<String, Integer> getTopicPartition(String topicPartition) {
        int index = topicPartition.lastIndexOf('-');
        Map<String, Integer> map = new HashMap<>();
        map.put(topicPartition.substring(0, index), Integer.parseInt(topicPartition.substring(index + 1)));
        return map;
    }

    // 获取异常堆栈跟踪的方法
    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    // 解析CSV字符串并返回映射的方法
    public static <K, V> Map<K, V> getCSVMap(String allCSVals, String exceptionMsg, String successMsg) {
        Map<K, V> map = new HashMap<>();
        if (!"".equals(allCSVals)) {
            String[] csVals = allCSVals.split(",");
            for (String val : csVals) {
                try {
                    String[] tempSplit = val.split(":");
                    logger.info(successMsg + tempSplit[0] + " : " + Integer.parseInt(tempSplit[1].trim()));
                    map.put((K) tempSplit[0], (V) tempSplit[1]);
                } catch (Throwable t) {
                    logger.error(exceptionMsg + ": " + val);
                }
            }
        }
        return map;
    }

    // 解析CSV列表的方法
    public static List<String> getCSVList(String csvList) {
        return (csvList == null) ? Collections.emptyList() : Arrays.asList(csvList.split(","));
    }

    // 根据属性字符串获取主题保留时间的方法
    public static Map<String, Integer> getTopicRentionHours(String retentionHours) {
        String exceptionMsg = "Malformed token for topic.log.retention.hours in server.properties: ";
        String successMsg = "The retention hour for ";
        return getCSVMap(retentionHours, exceptionMsg, successMsg);
    }

    // 根据属性字符串获取主题刷新间隔的方法
    public static Map<String, Integer> getTopicFlushIntervals(String allIntervals) {
        String exceptionMsg = "Malformed token for topic.flush.Intervals.ms in server.properties: ";
        String successMsg = "The flush interval for ";
        return getCSVMap(allIntervals, exceptionMsg, successMsg);
    }

    // 根据属性字符串获取主题分区数的方法
    public static Map<String, Integer> getTopicPartitions(String allPartitions) {
        String exceptionMsg = "Malformed token for topic.partition.counts in server.properties: ";
        String successMsg = "The number of partitions for topic ";
        return getCSVMap(allPartitions, exceptionMsg, successMsg);
    }

    // 根据属性字符串获取消费者主题映射的方法
    public static Map<String, Integer> getConsumerTopicMap(String consumerTopicString) {
        String exceptionMsg = "Malformed token for embeddedconsumer.topics in consumer.properties: ";
        String successMsg = "The number of consumer thread for topic ";
        return getCSVMap(consumerTopicString, exceptionMsg, successMsg);
    }

    // 根据类名创建对象实例的方法
    public static <T> T getObject(String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) Class.forName(className);
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            logger.error("Error creating object for class: {}", className, e);
            return null;
        }
    }

    // 检查属性是否存在的方法
    public static boolean propertyExists(String prop) {
        return prop != null && !prop.isEmpty();
    }
    /**
     * 根据属性获取压缩编解码器。
     * @param props 包含压缩编解码器属性的Properties对象
     * @param codec 压缩编解码器属性的名称
     * @return 压缩编解码器实例
     */
    public static CompressionCodec getCompressionCodec(Properties props, String codec) {
        String codecValueString = props.getProperty(codec);
        // 如果属性不存在或为空，则返回无压缩编解码器
        if (codecValueString == null || codecValueString.isEmpty()) {
            return new NoCompressionCodec();
        } else {
            // 否则，根据属性值获取相应的压缩编解码器
            return CompressionCodec.getCompressionCodec(Integer.parseInt(codecValueString));
        }
    }
}