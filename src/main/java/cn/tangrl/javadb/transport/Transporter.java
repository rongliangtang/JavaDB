package cn.tangrl.javadb.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * Transporter类
 * Encoder.code编码之后的信息会通过 Transporter 类，写入输出流发送出去。
 * 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符。
 * 这样在发送和接收数据时，就可以很简单地使用 BufferedReader 和 Writer 来直接按行读写了。
 */
public class Transporter {
    /**
     * socket对象
     */
    private Socket socket;
    /**
     * BufferedReader对象
     */
    private BufferedReader reader;
    /**
     * BufferedWriter对象
     */
    private BufferedWriter writer;

    /**
     * 构造函数，传入socker对象
     * @param socket
     * @throws IOException
     */
    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        // 获取socket的输入流
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        // 获取socket的输出流
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 发送传参dota数据，通过socket的输出流实现
     * @param data
     * @throws Exception
     */
    public void send(byte[] data) throws Exception {
        // 为了避免特殊字符造成问题，，将传入的buf byte数组转换成16进制的字符串
        String raw = hexEncode(data);
        writer.write(raw);
        // flush会将缓存区的数据强制输出
        writer.flush();
    }

    /**
     * 接收服务器返回的数据，通过socket的输入流实现
     * @return
     * @throws Exception
     */
    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if(line == null) {
            close();
        }
        // 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符
        return hexDecode(line);
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 将传入的buf byte数组转换成16进制的字符串
     * @param buf
     * @return
     */
    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true)+"\n";
    }

    /**
     * 将传入的16进制字符串转换成byte数组
     * @param buf
     * @return
     * @throws DecoderException
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
