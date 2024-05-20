package cn.tangrl.javadb.client;

import cn.tangrl.javadb.transport.Package;
import cn.tangrl.javadb.transport.Packager;

/**
 * 实现客户端的单次收发动作
 */
public class RoundTripper {
    /**
     * packager对象
     */
    private Packager packager;

    /**
     * 构造函数
     * @param packager
     */
    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 发送并接收包，返回接收的package对象
     * @param pkg
     * @return
     * @throws Exception
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    /**
     * 关闭资源
     * @throws Exception
     */
    public void close() throws Exception {
        packager.close();
    }
}
