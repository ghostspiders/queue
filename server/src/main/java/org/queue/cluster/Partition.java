package org.queue.cluster;

public class Partition implements Comparable<Partition> {

    private final int brokerId;
    private final int partId;

    // 主构造函数
    public Partition(int brokerId, int partId) {
        this.brokerId = brokerId;
        this.partId = partId;
    }

    // 使用字符串解析为Partition
    public static Partition parse(String str) {
        String[] pieces = str.split("-");
        if (pieces.length != 2) {
            throw new IllegalArgumentException("Expected name in the form x-y.");
        }
        return new Partition(Integer.parseInt(pieces[0]), Integer.parseInt(pieces[1]));
    }

    // 获取分区名称
    public String getName() {
        return brokerId + "-" + partId;
    }

    // 实现Comparable接口的compareTo方法
    @Override
    public int compareTo(Partition that) {
        if (this.brokerId != that.brokerId) {
            return this.brokerId - that.brokerId;
        } else {
            return this.partId - that.partId;
        }
    }

    // 重写toString方法
    @Override
    public String toString() {
        return getName();
    }

    // 重写equals方法
    @Override
    public boolean equals(Object other) {
        if (other instanceof Partition) {
            Partition partition = (Partition) other;
            return  (brokerId == partition.brokerId) && (partId == partition.partId);
        }else {
            return false;
        }
    }

    // 重写hashCode方法
    @Override
    public int hashCode() {
        return 31 * (17 + brokerId) + partId;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getPartId() {
        return partId;
    }
}