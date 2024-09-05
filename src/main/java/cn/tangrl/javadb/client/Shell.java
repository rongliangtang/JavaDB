package cn.tangrl.javadb.client;

import java.util.Scanner;

/**
 * 客户端命令行处理类
 */
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    /**
     * 持续获取用户输入，发送给服务器执行。
     * 每次扫描一行，以\n结尾。
     */
    public void run() {
        // 打印tip
        displayWelcomeMessage();

        Scanner sc = new Scanner(System.in);
        try {
            while(true) {
                System.out.print(":> ");
                String statStr = sc.nextLine();
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

            }
        } finally {
            sc.close();
            client.close();
        }
    }

    private static void displayWelcomeMessage() {
        String message =
                        "*********************************************************************\n" +
                        "*                      欢迎使用JavaDB!                               \n" +
                        "*********************************************************************\n" +
                        "* 数据库已经成功创建                                                   \n" +
                        "* 你现在可以使用下列命令                                                \n" +
                        "*                                                                   \n" +
                        "* 命令示例:                                                          \n" +
                        "* 1. 基本操作                                                        \n" +
                        "* (1) create table test_table id int32, value int32 (index id)      \n" +
                        "* (2) insert into test_table values 10 33                           \n" +
                        "* (3) select * from test_table where id=10                          \n" +
                        "* (4) delete from test_table where id=10                            \n" +
                        "* 2. 事务操作                                                        \n" +
                        "* (1) begin                                                         \n" +
                        "* (2) insert into test_table values 20 53                           \n" +
                        "* (3) select * from test_table where id=20                          \n" +
                        "* (4) commit                                                        \n" +
                        "*                                                                   \n" +
                        "*********************************************************************\n";

        System.out.println(message);
    }
}
