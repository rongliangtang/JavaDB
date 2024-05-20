package cn.tangrl.javadb.transport;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import cn.tangrl.javadb.common.Error;

/**
 * Encoder类
 * 实现Package的encode和decode
 * MYDB 使用了一种特殊的二进制格式，用于客户端和服务端通信。
 * 传输的最基本结构，是 Package对象。
 * 每个 Package 在发送前，由 Encoder 编码为字节数组，在对方收到后同样会由 Encoder 解码成 Package 对象。
 * 编码和解码的规则如下：[Flag][data]
 * 若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；如果 flag 为 1，表示发送的是错误，data 是 Exception.getMessage() 的错误提示信息。
 */
public class Encoder {
    /**
     * 编码
     * 将package对象编码成字节数组
     * @param pkg
     * @return
     */
    public byte[] encode(Package pkg) {
        // 如果err非空，编码成[1][err_msg]
        if(pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            // 如果err为空，编码成[0][data]
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 解密
     * 将字节数组解码成package对象
     * @param data
     * @return
     * @throws Exception
     */
    public Package decode(byte[] data) throws Exception {
        if(data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        // 如果flage==0，则利用data包裹成package对象
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            // 如果flage==1，则利用err包裹成package对象
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            // 否则，抛出异常
            throw Error.InvalidPkgDataException;
        }
    }

}
