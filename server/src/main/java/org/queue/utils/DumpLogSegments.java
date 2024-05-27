package org.queue.utils;

import org.queue.message.FileMessageSet;
import org.queue.message.MessageAndOffset;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class DumpLogSegments {

    public static void main(String[] args) {
        boolean isNoPrint = false;
        // 检查命令行参数，设置是否打印消息内容
        for (String arg : args) {
            if ("-noprint".equalsIgnoreCase(arg)) {
                isNoPrint = true;
            }
        }

        for (String arg : args) {
            // 跳过 "-noprint" 参数
            if (!"-noprint".equalsIgnoreCase(arg)) {
                File file = new File(arg);
                System.out.println("Dumping " + file);

                // 假设文件名的格式是 "offsetnumber.log"
                String[] parts = file.getName().split("\\.");
                long offset = Long.parseLong(parts[0]);
                System.out.println("Starting offset: " + offset);

                // 假设 FileMessageSet 是一个处理文件消息的类
                FileMessageSet messageSet = new FileMessageSet(file, false);
                for (MessageAndOffset messageAndOffset : messageSet) {
                    System.out.println("----------------------------------------------");
                    if (messageAndOffset.getMessage().isValid()) {
                        System.out.println("offset:\t" + offset);
                    } else {
                        System.out.println("offset:\t " + offset + "\t invalid");
                    }
                    if (!isNoPrint) {
                        // 假设 Utils.toString 是一个工具方法，用于将字节数组转换为字符串
                        System.out.println("payload:\t" + Utils.toString(messageAndOffset.getMessage().payload(), StandardCharsets.UTF_8));
                    }
                    // 更新 offset
                    offset += messageAndOffset.offset();
                }
            }
        }
    }

}