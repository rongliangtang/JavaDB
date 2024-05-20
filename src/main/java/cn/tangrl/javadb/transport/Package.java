package cn.tangrl.javadb.transport;

/**
 * Package类
 * MYDB 使用了一种特殊的二进制格式，用于客户端和服务端通信。
 * 传输的最基本结构，是 Package对象。
 * 每个 Package 在发送前，由 Encoder 编码为字节数组，在对方收到后同样会由 Encoder 解码成 Package 对象。
 * 编码和解码的规则如下：[Flag][data]
 * 若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；如果 flag 为 1，表示发送的是错误，data 是 Exception.getMessage() 的错误提示信息。
 */
public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
