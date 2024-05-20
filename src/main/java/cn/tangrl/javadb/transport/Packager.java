package cn.tangrl.javadb.transport;

/**
 * Packager 则是 Encoder 和 Transporter 的结合体，直接对外提供 send 和 receive 方法
 */
public class Packager {
    /**
     * Transporter对象
     */
    private Transporter transpoter;
    /**
     * Encoder对象
     */
    private Encoder encoder;

    /**
     * 构造函数
     * @param transpoter
     * @param encoder
     */
    public Packager(Transporter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }

    /**
     * 发送Package数据给服务器
     * 传入Package对象
     * @param pkg
     * @throws Exception
     */
    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transpoter.send(data);
    }

    /**
     * 接收服务器返回的Package数据
     * 返回Package对象
     * @return
     * @throws Exception
     */
    public Package receive() throws Exception {
        byte[] data = transpoter.receive();
        return encoder.decode(data);
    }

    /**
     * 关闭资源
     * @throws Exception
     */
    public void close() throws Exception {
        transpoter.close();
    }
}
