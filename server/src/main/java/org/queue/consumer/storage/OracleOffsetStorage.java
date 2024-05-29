package org.queue.consumer.storage;

/**
 * @author gaoyvfeng
 * @ClassName OracleOffsetStorage
 * @description:
 * @datetime 2024年 05月 22日 16:31
 * @version: 1.0
 */
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.sql.*;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * 使用Oracle数据库保存偏移量的OffsetStorage实现。
 */
public class OracleOffsetStorage implements OffsetStorage {

    private final Connection connection; // 数据库连接对象
    private final Logger logger = LoggerFactory.getLogger(OracleOffsetStorage.class); // 日志对象
    private final Object lock = new Object(); // 对象锁

    /**
     * 构造函数，传入数据库连接。
     */
    public OracleOffsetStorage(Connection connection) throws SQLException {
        this.connection = connection;
        this.connection.setAutoCommit(false); // 设置为手动提交事务
    }

    @Override
    public long reserve(int node, String topic) {
        // 尝试获取并锁定偏移量，如果不存在，则创建它
        Long offset = selectExistingOffset(connection, node, topic)
                .orElseGet(() -> {
                    maybeInsertZeroOffset(connection, node, topic);
                    return selectExistingOffset(connection, node, topic)
                            .orElseThrow(() -> new NoSuchElementException("Offset not found"));
                });

        // 如果调试模式开启，记录预留的偏移量信息
        if (logger.isDebugEnabled()) {
            logger.debug("Reserved node " + node + " for topic '" + topic + " offset " + offset);
        }
        return offset;
    }

    @Override
    public void commit(int node, String topic, long offset) {
        boolean success = false;
        try {
            updateOffset(connection, node, topic, offset);
            success = true;
        } finally {
            commitOrRollback(connection, success);
        }

        // 如果调试模式开启，记录提交的偏移量信息
        if (logger.isDebugEnabled()) {
            logger.debug("Updated node " + node + " for topic '" + topic + "' to " + offset);
        }
    }

    /**
     * 关闭数据库连接。
     */
    public void close() {
        // 此处假设Utils.swallow方法可以正确关闭资源并处理可能的异常
        try {
            connection.close();
        } catch (SQLException e) {
            Utils.swallow(Level.ERROR, e);
        }
    }

    /**
     * 如果数据库中不存在指定节点和主题的偏移量记录，则插入一条新记录，偏移量初始值为0。
     * @param connection 数据库连接对象
     * @param node 节点标识符
     * @param topic 主题名称
     * @return 如果插入了新记录，则返回true；否则返回false
     */
    private boolean maybeInsertZeroOffset(Connection connection, int node, String topic) {
        // 准备SQL语句，使用占位符?来防止SQL注入
        String sql = "insert into queue_offsets (node, topic, offset) " +
                "select ?, ?, 0 from dual where not exists " +
                "(select null from queue_offsets where node = ? and topic = ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            // 设置参数值
            stmt.setInt(1, node);
            stmt.setString(2, topic);
            stmt.setInt(3, node);
            stmt.setString(4, topic);

            // 执行更新操作，并获取受影响的行数
            int updated = stmt.executeUpdate();

            // 如果更新的行数大于1，则抛出异常，因为这表明不应该有多个记录被更新
            if (updated > 1) {
                throw new IllegalStateException("More than one key updated by primary key!");
            } else {
                // 返回受影响的行数是否为1，即是否插入了新记录
                return updated == 1;
            }
        } catch (SQLException e) {
            // 处理可能的SQLException
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 查询数据库中指定节点和主题的现有偏移量。
     * @param connection 数据库连接对象
     * @param node 节点标识符
     * @param topic 主题名称
     * @return 如果找到记录，则返回Optional包裹的偏移量；如果没有找到，则返回Optional.empty()
     */
    private Optional<Long> selectExistingOffset(Connection connection, int node, String topic) {
        // 准备SQL查询语句，使用占位符?来防止SQL注入
        String sql = "select offset from queue_offsets where node = ? and topic = ? for update";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            // 设置查询参数
            stmt.setInt(1, node);
            stmt.setString(2, topic);

            // 执行查询
            try (ResultSet results = stmt.executeQuery()) {
                // 如果没有查询结果，返回Optional.empty()
                if (!results.next()) {
                    return Optional.empty();
                } else {
                    // 如果查询到结果，获取偏移量
                    long offset = results.getLong("offset");
                    // 如果存在多条记录，抛出异常
                    if (results.next()) {
                        throw new IllegalStateException("More than one entry for primary key!");
                    }
                    // 返回包含偏移量的Optional
                    return Optional.of(offset);
                }
            }
        } catch (SQLException e) {
            // 处理可能的SQLException
            e.printStackTrace();
            return Optional.empty();
        }
    }


    /**
     * 更新数据库中指定节点和主题的偏移量。
     * @param connection 数据库连接对象
     * @param node 节点标识符
     * @param topic 主题名称
     * @param newOffset 新的偏移量值
     */
    private void updateOffset(Connection connection, int node, String topic, long newOffset) {
        String sql = "update queue_offsets set offset = ? where node = ? and topic = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, newOffset);
            stmt.setInt(2, node);
            stmt.setString(3, topic);

            int updated = stmt.executeUpdate();
            // 检查是否恰好更新了一行
            if (updated != 1) {
                throw new IllegalStateException("Unexpected number of keys updated: " + updated);
            }
        } catch (SQLException e) {
            // 日志异常信息
            logger.error("Error updating offset in database", e);
        }
    }

    /**
     * 根据提交参数决定提交或回滚数据库事务。
     * @param connection 数据库连接对象
     * @param commit 是否提交事务
     */
    private void commitOrRollback(Connection connection, boolean commit) {
        if (connection != null) {
            try {
                if (commit) {
                    connection.commit();
                } else {
                    connection.rollback();
                }
            } catch (SQLException e) {
                // 日志异常信息
                logger.error("Error during commit or rollback", e);
            }
        }
    }

    /**
     * 关闭ResultSet并处理可能的异常。
     * @param rs 要关闭的ResultSet对象
     */
    private void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // 日志异常信息
                logger.error("Error closing ResultSet", e);
            }
        }
    }

    /**
     * 关闭PreparedStatement并处理可能的异常。
     * @param stmt 要关闭的PreparedStatement对象
     */
    private void close(PreparedStatement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                // 日志异常信息
                logger.error("Error closing PreparedStatement", e);
            }
        }
    }

    /**
     * 关闭数据库连接并处理可能的异常。
     * @param connection 要关闭的数据库连接对象
     */
    private void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // 日志异常信息
                logger.error("Error closing Connection", e);
            }
        }
    }
}
