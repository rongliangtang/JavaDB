package cn.tangrl.javadb.backend.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import cn.tangrl.javadb.backend.tbm.TableManager;
import cn.tangrl.javadb.transport.Encoder;
import cn.tangrl.javadb.transport.Package;
import cn.tangrl.javadb.transport.Packager;
import cn.tangrl.javadb.transport.Transporter;

/**
 * Sever类
 * 使用 Java 的 socket实现mydb的服务器
 */
public class Server {
    /**
     * server的端口
     */
    private int port;
    /**
     * tbm模块对象
     */
    TableManager tbm;

    /**
     * 构造函数
     * @param port
     * @param tbm
     */
    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    /**
     * 启动服务器
     */
    public void start() {
        // 在指定端口上创建一个 ServerSocket 对象，用于监听客户端的连接
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        // 创建一个线程池，用于处理多个客户端的并发连接和请求。
        // 核心线程数为 10，最大线程数为 20，空闲线程的存活时间为 1 秒，使用 ArrayBlockingQueue 作为工作队列，容量为 100。
        // 当线程池被耗尽时，使用 CallerRunsPolicy，该策略会由调用线程执行任务。
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            // 进入一个无限循环，持续接受客户端连接。
            while(true) {
                Socket socket = ss.accept();
                // 创建任务并执行
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }
}

/**
 * HandleSocket 类实现了 Runnable 接口，用于处理每个客户端连接的任务。
 * 该类的主要职责是管理与客户端的通信，接收 SQL 查询，执行查询并返回结果。
 */
class HandleSocket implements Runnable {
    /**
     * socket对象
     */
    private Socket socket;
    /**
     * tbm模块对象
     */
    private TableManager tbm;

    /**
     * 构造函数
     * @param socket
     * @param tbm
     */
    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    /**
     * run 方法是 Runnable 接口的实现，包含处理客户端请求的逻辑。
     */
    @Override
    public void run() {
        // 获取并打印客户端地址
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());
        // 初始化通信组件
        Packager packager = null;
        try {
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch(IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        // 创建执行器对象
        Executor exe = new Executor(tbm);
        // 持续接收命令并执行
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}