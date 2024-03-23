package top.lxsky711.easydb.core.dm.logger;

/**
 * @Author: 711lxsky
 * @Description: 每次对底层数据操作时，都会记录一条日志在磁盘上
 * 以供数据库崩溃之后，恢复数据使用
 * <p>
 * 日志文件记录格式：
 * [LogChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * LogChecksum 为后续所有日志计算的Checksum，4字节int类型
 * Log1...LogN是常规日志数据
 * BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在
 * </p>
 * <p>
 * 单条日志记录格式：
 * [Size][Checksum][Data]
 * Size标记Data字段的字节数
 * Checksum是该条数据的校验和
 * Data是实际的数据
 * </P>
 */

public interface Logger {



}
