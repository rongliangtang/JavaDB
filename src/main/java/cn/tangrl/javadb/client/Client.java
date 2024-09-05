package cn.tangrl.javadb.client;

import cn.tangrl.javadb.transport.Package;
import cn.tangrl.javadb.transport.Packager;

/**
 * 客户端启动类
 */
public class Client {
    /**
     * 实现客户端的单次收发动作的类
     */
    private RoundTripper rt;

    /**
     * 构造函数
     * @param packager
     */
    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    /**
     * 接收用户输入的stat，并发送给服务器，然后接收服务器返回的结果
     * @param stat
     * @return
     * @throws Exception
     */
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    /**
     * 关闭资源
     */
    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
