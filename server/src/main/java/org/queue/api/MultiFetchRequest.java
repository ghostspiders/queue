package org.queue.api;

import org.queue.network.Request;
import org.queue.utils.SingleUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MultiFetchRequest extends Request {
    private List<FetchRequest> fetches;

    // 私有构造函数，由伴生对象的方法调用
    private MultiFetchRequest(List<FetchRequest> fetches) {
        super(RequestKeys.multiFetch);
        this.fetches = fetches;
    }

    //从ByteBuffer中读取数据创建MultiFetchRequest对象
    public  MultiFetchRequest readFrom(ByteBuffer buffer) {
        int count = buffer.getShort() & 0xFFFF; // 将short转换为int
        List<FetchRequest> fetchList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            fetchList.add(SingleUtil.FetchRequest.readFrom(buffer));
        }
        return new MultiFetchRequest(fetchList);
    }

    // 将MultiFetchRequest对象的数据写入ByteBuffer
    public void writeTo(ByteBuffer buffer) {
        if (fetches.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Number of requests in MultiFetchRequest exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) fetches.size());
        for (FetchRequest fetch : fetches) {
            fetch.writeTo(buffer);
        }
    }

    // 计算请求的字节大小
    public int sizeInBytes() {
        int size = 2; // size of the short count
        for (FetchRequest fetch : fetches) {
            size += fetch.sizeInBytes();
        }
        return size;
    }

    // 重写toString方法
    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (FetchRequest fetch : fetches) {
            buffer.append(fetch.toString());
            buffer.append(",");
        }
        // Remove the last comma if there are any fetches
        if (!fetches.isEmpty()) {
            buffer.deleteCharAt(buffer.length() - 1);
        }
        return "MultiFetchRequest{" + buffer.toString() + "}";
    }
}