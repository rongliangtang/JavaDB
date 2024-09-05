package cn.tangrl.javadb.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import cn.tangrl.javadb.transport.Encoder;
import cn.tangrl.javadb.transport.Packager;
import cn.tangrl.javadb.transport.Transporter;

/**
 * 客户端的启动入口
 */
public class Launcher {
    /**
     * 客户端的主线程
     * @param args
     * @throws UnknownHostException
     * @throws IOException
     */
    public static void main(String[] args) throws UnknownHostException, IOException {
        // 创建相应的对象
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        // 创建shell对象，run shell 会持续接收客户端的输入并发送给服务端处理，并输出服务端返回的结果。
        Shell shell = new Shell(client);
        shell.run();
    }
}
