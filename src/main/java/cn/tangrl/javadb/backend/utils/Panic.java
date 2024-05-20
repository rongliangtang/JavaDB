package cn.tangrl.javadb.backend.utils;

/**
 * 处理异常类
 */
public class Panic {
    /**
     * 处理异常方法
     * 接收异常，打印错误堆栈跟踪信息，并异常退出程序
     * @param err
     */
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
