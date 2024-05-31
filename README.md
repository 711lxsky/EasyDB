# EasyDB

更了一篇博文：[EasyDB项目回顾](https://711lxsky.github.io/2024/05/31/blog21/)

一个`Java`实现的数据库，部分原理参照自`MySQL`、`PostgreSQL`和`SQLite`，实现了以下比较重要功能：

- 可靠性数据检测和数据丢失恢复策略
- 两段锁协议（2PL）实现可串行化调度
- MVCC多版本并发控制
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 封装式表字段管理
- 基于`socket`的`server`服务端和`client`客户端通信

## 运行方式

注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先执行以下命令编译源码：

```shell
mvn compile
```

接着执行以下命令以 /tmp/easydb/example 作为路径创建数据库：

`pom.xml`中没有指定主类，就是为了方便运行`Server.Launcher`或者`Client.Launcher`

```shell
mvn exec:java -Dexec.mainClass="top.lxsky711.easydb.server.Launcher" -Dexec.args="-create /tmp/easydb/example"
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -Dexec.mainClass="top.lxsky711.easydb.server.Launcher" -Dexec.args="-open /tmp/easydb/example"
```

这时数据库服务就已经启动在本机的 9875 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -Dexec.mainClass="top.lxsky711.easydb.client.Launcher"
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

