package org.queue.log;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.queue.api.OffsetRequest;
import org.queue.common.OffsetOutOfRangeException;
import org.queue.message.FileMessageSet;
import org.queue.message.InvalidMessageException;
import org.queue.message.MessageAndOffset;
import org.queue.message.MessageSet;
import org.queue.utils.Range;
import org.queue.utils.SystemTime;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个用于存储消息的追加日志类。
 */
public class Log {
    private static final Logger logger = LoggerFactory.getLogger(Log.class);

    // 日志目录
    private final File dir;
    // 日志段的最大大小
    private final long maxSize;
    // 刷新间隔，即在自动刷新到磁盘之前可以追加的消息数量
    private final int flushInterval;
    // 是否需要恢复
    private final boolean needRecovery;

    // 保护日志所有修改的锁
    private final Object lock = new Object();

    // 未刷新到磁盘的消息数量
    private final AtomicInteger unflushed = new AtomicInteger(0);

    // 最后一次刷新的时间
    private final AtomicLong lastflushedTime = new AtomicLong(System.currentTimeMillis());

    // 日志段列表
    private SegmentList<LogSegment> segments;

    // 日志名称
    private final String name;

    // 日志统计信息
    private final LogStats logStats;
    public static final String FileSuffix = ".queue";

    /**
     * 构造函数。
     * @param dir 日志目录。
     * @param maxSize 日志段的最大大小。
     * @param flushInterval 刷新间隔。
     * @param needRecovery 是否需要恢复。
     */
    public Log(File dir, long maxSize, int flushInterval, boolean needRecovery) throws IOException {
        this.dir = dir;
        this.maxSize = maxSize;
        this.flushInterval = flushInterval;
        this.needRecovery = needRecovery;
        this.name = dir.getName();
        this.segments = loadSegments();
        this.logStats = new LogStats(this);
        Utils.registerMBean(logStats, "queue:type=queue.logs." + this.name);
    }
    /**
     * 在一系列范围对象中通过值查找给定的范围对象。执行二分查找，
     * 但是不是检查等式，而是检查值是否在范围内。
     * 可以指定数组大小，以防在搜索过程中数组增长。
     *
     * @param ranges 范围数组
     * @param value 要查找的值
     * @param arraySize 数组的大小
     * @param <T> 范围类型，必须实现Comparable接口
     * @return 如果找到则返回Optional包装的范围对象，否则返回空的Optional
     */
    @SuppressWarnings("unchecked")
    public static <T extends Range> Optional<T> findRange(T[] ranges, long value, int arraySize) {
        if (ranges.length < 1) {
            return Optional.empty();
        }

        // 检查是否越界
        if (value < ranges[0].start() || value > ranges[arraySize - 1].start() + ranges[arraySize - 1].size()) {
            throw new OffsetOutOfRangeException("offset " + value + " is out of range");
        }

        // 检查是否在最后一个范围的末尾
        if (value == ranges[arraySize - 1].start() + ranges[arraySize - 1].size()) {
            return Optional.empty();
        }

        int low = 0;
        int high = arraySize - 1;
        while (low <= high) {
            int mid = (high + low) / 2;
            T found = ranges[mid];
            if (found.contains(value)) {
                return Optional.of(found);
            } else if (value < found.start()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return Optional.empty();
    }

    /**
     * 简化版的findRange方法，使用数组的自然长度作为数组大小参数。
     *
     * @param ranges 范围数组
     * @param value 要查找的值
     * @param <T> 范围类型，必须实现Comparable接口
     * @return 如果找到则返回Optional包装的范围对象，否则返回空的Optional
     */
    public static <T extends Range> Optional<T> findRange(T[] ranges, long value) {
        return findRange(ranges, value, ranges.length);
    }

    /**
     * 根据偏移量生成日志分段文件名。这会将偏移量数字用零填充，
     * 以确保文件在数字排序时能正确排序。
     *
     * @param offset 偏移量
     * @return 生成的文件名
     */
    public static String nameFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset) + FileSuffix;
    }
    // 加载日志分段的方法
    private SegmentList<LogSegment> loadSegments() throws IOException {
        File[] files = dir.listFiles((d, name) -> name.endsWith(Log.FileSuffix));
        if (files == null) {
            throw new IOException("Cannot read directory: " + dir.getAbsolutePath());
        }

        List<LogSegment> segments = new ArrayList<>();
        for (File file : files) {
            if (!file.canRead()) {
                throw new IOException("Cannot read file: " + file.getAbsolutePath());
            }
            String filename = file.getName();
            long start = Long.parseLong(filename.substring(0, filename.length() - Log.FileSuffix.length()));
            FileMessageSet messageSet = new FileMessageSet(file, false);
            segments.add(new LogSegment(file, messageSet, start));
        }

        if (segments.isEmpty()) {
            File newFile = new File(dir, Log.nameFromOffset(0));
            FileMessageSet set = new FileMessageSet(newFile, true);
            segments.add(new LogSegment(newFile, set, 0));
        } else {
            Collections.sort(segments, Comparator.comparingLong(s -> s.start()));
            validateSegments(segments);
            LogSegment last = segments.remove(segments.size() - 1);
            last.getMessageSet().close();
            FileMessageSet newMessageSet = new FileMessageSet(last.getFile(), true, new AtomicBoolean(needRecovery));
            LogSegment mutable = new LogSegment(last.getFile(), newMessageSet, last.getStart());
            segments.add(mutable);
        }

        return new SegmentList<>(segments);
    }
    // 验证日志分段的方法
    private void validateSegments(List<LogSegment> segments) {
        synchronized (lock) {
            for (int i = 0; i < segments.size() - 1; i++) {
                LogSegment curr = segments.get(i);
                LogSegment next = segments.get(i + 1);
                if (curr.getStart() + curr.size() != next.getStart()) {
                    throw new IllegalStateException("Segments do not validate: "
                            + curr.getFile().getAbsolutePath() + " and "
                            + next.getFile().getAbsolutePath());
                }
            }
        }
    }
    // 获取日志分段的数量
    public int numberOfSegments() {
        return segments.view().length;
    }
    // 关闭日志，释放资源
    public void close() {
        synchronized (lock) {
            for (LogSegment segment : segments.view()) {
                segment.getMessageSet().close();
            }
        }
    }
    /**
     * 追加消息集到日志中。
     * @param messages 要追加的消息集。
     */
    public void append(MessageSet messages) {
        // 验证消息
        int numberOfMessages = 0;
        for (MessageAndOffset messageAndOffset : messages) {
            if (!messageAndOffset.getMessage().isValid()) {
                throw new InvalidMessageException();
            }
            numberOfMessages++;
        }

        // 记录统计信息
        logStats.recordAppendedMessages(numberOfMessages);

        // 它们是有效的，将它们插入到日志中
        synchronized (lock) {
            LogSegment segment = segments.view()[segments.view().length - 1];
            segment.getMessageSet().append(messages);
            maybeFlush(numberOfMessages);
            maybeRoll(segment);
        }
    }
    // 从日志读取消息集
    public MessageSet read(long offset, int length) {
        LogSegment segment = findSegment(offset);
        if (segment != null) {
            return segment.getMessageSet().read((int) (offset - segment.getStart()), length);
        }
        return MessageSet.Empty;
    }

    private LogSegment findSegment(long offset) {
        for (LogSegment segment : segments.view()) {
            if (segment.contains(offset)) {
                return segment;
            }
        }
        return null;
    }
    // 标记删除满足条件的日志分段
    public List<LogSegment> markDeletedWhile(Predicate<LogSegment> predicate) {
        synchronized (lock) {
            List<LogSegment> deletableSegments = new ArrayList<>();
            for (LogSegment segment : segments.view()) {
                if (predicate.test(segment)) {
                    segment.setDeleted(true);
                    deletableSegments.add(segment);
                }
            }
            int numToDelete = deletableSegments.size();
            if (numToDelete == segments.view().length) {
                roll();
            }
            return Arrays.asList(segments.trunc(numToDelete));
        }
    }
    // 获取日志大小
    public long size() {
        long totalSize = 0;
        for (LogSegment segment : segments.view()) {
            totalSize += segment.size();
        }
        return totalSize;
    }
    // 获取下一次追加消息的字节偏移量
    public long nextAppendOffset() {
        flush();
        LogSegment last = segments.view()[segments.view().length-1];
        return last.getStart() + last.size();
    }

    /**
     * 获取最后一个Segment的高水位标记。
     * @return 高水位标记的long值
     */
    public long getHighwaterMark() {
        if (segments == null || segments.view().length == 0) {
            throw new IllegalArgumentException("Segment列表不能为空");
        }

        // 因为Java中的List是有序的，所以可以直接通过segments.size() - 1来访问最后一个元素
        LogSegment lastSegment = segments.view()[segments.view().length-1];
        FileMessageSet messageSet = lastSegment.getMessageSet();
        return messageSet.highWaterMark();
    }
    private void maybeRoll(LogSegment segment) {
        if (segment.getMessageSet().sizeInBytes() > maxSize) {
            roll();
        }
    }
    // 如果需要则滚动日志，创建新的日志段
    public void roll() {
        synchronized (lock) {
            File newFile = new File(dir, Log.nameFromOffset(nextAppendOffset()));
            FileMessageSet messageSet = new FileMessageSet(newFile, true);
            LogSegment newSegment = new LogSegment(newFile, messageSet, nextAppendOffset());
            segments.append(newSegment);
        }
    }
    // 创建新的日志分段并激活
    private void maybeFlush(int numberOfMessages) {
        if (unflushed.addAndGet(numberOfMessages) >= flushInterval) {
            flush();
        }
    }
    // 刷新日志，将内存中的数据写入到磁盘
    public void flush() {
        synchronized (lock) {
            LogSegment lastSegment = segments.view()[segments.view().length-1];
            lastSegment.getMessageSet().flush();
            unflushed.set(0);
            lastflushedTime.set(System.currentTimeMillis());
        }
    }
    /**
     * 根据给定的OffsetRequest获取指定时间点之前的所有偏移量。
     * @param request 包含时间点和最大偏移量数量的OffsetRequest。
     * @return 一个长整型数组，包含请求的偏移量。
     */
    public long[] getOffsetsBefore(OffsetRequest request) {
        List<LogSegment> segsArray = List.of(segments.view()); // 将segments转换为列表以支持索引操作
        long[][] offsetTimeArray;
        if (!segsArray.isEmpty() && segsArray.get(segsArray.size() - 1).size() > 0) {
            offsetTimeArray = new long[segsArray.size() + 1][2];
        } else {
            offsetTimeArray = new long[segsArray.size()][2];
        }

        // 填充偏移量数组
        for (int i = 0; i < segsArray.size(); i++) {
            LogSegment segment = segsArray.get(i);
            offsetTimeArray[i] = new long[]{segment.getStart(), segment.getFile().lastModified()};
        }
        // 如果最后一个Segment不为空，则添加额外的高水位标记
        if (!segsArray.isEmpty() && segsArray.get(segsArray.size() - 1).size() > 0) {
            long highWaterMark = segsArray.get(segsArray.size() - 1).getMessageSet().highWaterMark();
            offsetTimeArray[segsArray.size()] = new long[]{segsArray.get(segsArray.size() - 1).getStart() + highWaterMark, SystemTime.INSTANCE.milliseconds()};
        }

        // 根据请求的时间点找到起始索引
        int startIndex = -1;
        switch ((int) request.getTime()) {
            case (int) OffsetRequest.LatestTime:
                startIndex = offsetTimeArray.length - 1;
                break;
            case (int) OffsetRequest.EarliestTime:
                startIndex = 0;
                break;
            default:
                boolean isFound = false;
                if (logger.isDebugEnabled()) {
                    StringBuilder debugMessage = new StringBuilder("Offset time array = ");
                    for (long[] o : offsetTimeArray) {
                        debugMessage.append(String.format("%d, %d", o[0], o[1])).append(" ");
                    }
                    logger.debug(debugMessage.toString());
                }
                startIndex = offsetTimeArray.length - 1;
                // 从数组末尾向前找到第一个小于等于请求时间的时间戳
                while (startIndex >= 0 && !isFound) {
                    if (offsetTimeArray[startIndex][1] <= request.getTime()) {
                        isFound = true;
                    } else {
                        startIndex--;
                    }
                }
        }

        // 计算返回数组的大小，并根据请求的最大偏移量限制大小
        int retSize = Math.min(request.getMaxNumOffsets(), startIndex + 1);
        long[] ret = new long[retSize];
        // 填充返回数组
        for (int j = 0; j < retSize; j++) {
            ret[j] = offsetTimeArray[startIndex][0];
            startIndex--;
        }
        return ret;
    }

    /**
     * 从类名中提取主题名称。
     * @return 返回主题名称字符串。
     */
    public String getTopicName() {
        int lastHyphenIndex = name.lastIndexOf("-");
        return (lastHyphenIndex >= 0) ? name.substring(0, lastHyphenIndex) : name;
    }

    /**
     * 获取最后一次刷新的时间。
     * @return 返回最后一次刷新的时间为长整型值。
     */
    public long getLastFlushedTime() {
        return lastflushedTime.get();
    }

    public File getDir() {
        return dir;
    }
    public String getName() {
        return name;
    }
}

