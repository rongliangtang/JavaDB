package cn.tangrl.javadb.backend;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cn.tangrl.javadb.backend.dm.DataManager;
import cn.tangrl.javadb.backend.server.Server;
import cn.tangrl.javadb.backend.tbm.TableManager;
import cn.tangrl.javadb.backend.tm.TransactionManager;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.vm.VersionManager;
import cn.tangrl.javadb.backend.vm.VersionManagerImpl;
import cn.tangrl.javadb.common.Error;

/**
 * 启动服务器类
 */
public class Launcher {
    /**
     * 服务器端口
     */
    public static final int port = 9999;
    /**
     * 默认内存大小64MB
     */
    public static final long DEFALUT_MEM = (1<<20)*64;
    /**
     * 定义了 KB、MB 和 GB 的常量
     */
    public static final long KB = 1 << 10;
	public static final long MB = 1 << 20;
	public static final long GB = 1 << 30;

    /**
     * main线程
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException {
        // 创建cli库的Options对象，并定义命令行选项
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        // 使用 DefaultParser 创建命令行解析器
        CommandLineParser parser = new DefaultParser();
        // 解析命令行参数并存储在 CommandLine 对象 cmd 中。
        CommandLine cmd = parser.parse(options,args);

        // 根据cmd对象包含的选项，执行对应的数据库操作
        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    /**
     * 创建数据库，创建对应的文件对象后关闭
     * @param path
     */
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    /**
     * 打开数据库，启动服务器
     * @param path
     * @param mem
     */
    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    /**
     * 解析内存大小，返回数值，单位byte
     * @param memStr
     * @return
     */
    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
