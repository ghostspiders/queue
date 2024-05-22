package org.queue.common;


import org.queue.message.InvalidMessageException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
/**
 * @author gaoyvfeng
 * @ClassName ErrorMapping
 * @description:
 * @datetime 2024年 05月 22日 09:42
 * @version: 1.0
 */
public class ErrorMapping {
    // 定义一个字节缓冲区，大小为0
    public static final ByteBuffer EmptyByteBuffer = ByteBuffer.allocate(0);
    // 定义错误代码常量
    public static final int UnknownCode = -1;
    public static final int NoError = 0;
    public static final int OffsetOutOfRangeCode = 1;
    public static final int InvalidMessageCode = 2;
    public static final int WrongPartitionCode = 3;
    public static final int InvalidFetchSizeCode = 4;

    // 用于映射异常类到错误代码的静态变量
    private static final Map<Class<? extends Throwable>, Integer> exceptionToCode = new HashMap<>();
    // 用于映射错误代码回异常类的静态变量
    private static final Map<Integer, Class<? extends Throwable>> codeToException = new HashMap<>();

    // 静态初始化块，用于初始化映射关系
    static {
        // 添加异常类到错误代码的映射
        exceptionToCode.put(OffsetOutOfRangeException.class, OffsetOutOfRangeCode);
        exceptionToCode.put(InvalidMessageException.class, InvalidMessageCode);
        exceptionToCode.put(InvalidPartitionException.class, WrongPartitionCode);
        exceptionToCode.put(InvalidMessageSizeException.class, InvalidFetchSizeCode);
        // 添加默认的错误代码
        exceptionToCode.put(UnknownException.class, UnknownCode);

        // 根据异常类到错误代码的映射，创建错误代码到异常类反向映射
        for (Map.Entry<Class<? extends Throwable>, Integer> entry : exceptionToCode.entrySet()) {
            codeToException.put(entry.getValue(), entry.getKey());
        }
    }

    // 根据异常类获取错误代码的方法
    public static int codeFor(Class<? extends Throwable> exceptionClass) {
        // 返回异常类对应的错误代码，如果没有找到则返回未知错误代码
        return exceptionToCode.getOrDefault(exceptionClass, UnknownCode);
    }

    // 根据错误代码抛出相应异常的方法
    public static void maybeThrowException(int code) throws Throwable {
        // 根据错误代码获取异常类，如果没有找到则使用未知异常类
        Class<? extends Throwable> exceptionClass = codeToException.getOrDefault(code, UnknownException.class);
        try {
            // 通过反射创建异常实例并抛出
            Throwable throwable = exceptionClass.newInstance();
            throw throwable;
        } catch (InstantiationException | IllegalAccessException e) {
            // 如果无法创建异常实例，则抛出运行时异常
            throw new RuntimeException("Error instantiating exception", e);
        }
    }
}