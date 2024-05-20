# JavaDB

JavaDB 是一个 Java 实现的简单的数据库，部分原理参照自 MySQL、PostgreSQL 和 SQLite。实现了以下功能：

- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简陋的 SQL 解析（因为懒得写词法分析和自动机，就弄得比较简陋）
- 基于 socket 的 server 和 client

## 运行方式

注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先执行以下命令编译源码：

```shell
mvn compile
# 这个命令使用Maven编译项目源码。它会下载所需的依赖库，并将源码编译为字节码，放在target/classes目录中。
```

接着执行以下命令以 /tmp/javadb 作为路径创建数据库：

```shell
mvn exec:java -Dexec.mainClass="cn.tangrl.javadb.backend.Launcher" -Dexec.args="-create /tmp/javadb"
# 这个命令通过Maven执行指定的主类top.guoziyang.javadb.backend.Launcher，并传递参数-create /tmp/javadb来创建一个新的数据库。参数-create表示创建操作，/tmp/javadb是数据库的存储路径。
# mvn exec:java: 使用Maven执行Java程序。
# 使用 mvn exec:java 可以简化命令行的复杂度。比如，如果直接使用 java 命令运行程序，你可能需要手动指定所有依赖的 JAR 文件和类路径
# -Dexec.mainClass="cn.tangrl.javadb.backend.Launcher": 指定要执行的主类。
# -Dexec.args="-create /tmp/javadb": 传递给主类的参数
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -Dexec.mainClass="cn.tangrl.javadb.backend.Launcher" -Dexec.args="-open /tmp/javadb"
# 这个命令通过Maven执行同一个主类，但传递的参数是-open /tmp/javadb，表示打开之前创建的数据库并启动数据库服务。
# 默认情况下，数据库服务会在本机的9999端口上运行。
```

这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -Dexec.mainClass="cn.tangrl.javadb.client.Launcher"
# 这个命令通过Maven执行客户端主类top.guoziyang.javadb.client.Launcher，启动一个交互式命令行界面。
# 用户可以在这个命令行界面中输入类似SQL的语法，连接到本地运行的数据库服务并执行SQL语句。
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

一个执行示例：

![](https://s3.bmp.ovh/imgs/2021/11/2749906870276904.png)

## 参考
[mydb](https://github.com/CN-GuoZiyang/MYDB)