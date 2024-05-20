package cn.tangrl.javadb.backend.parser;

import cn.tangrl.javadb.common.Error;

/**
 * Tokenizer 分词器类，对语句进行逐字节解析，根据空白符或者上述词法规则，将语句切割成多个 token。
 * 对外提供了 peek()、pop() 方法方便取出 Token 进行解析。切
 */
public class Tokenizer {
    /**
     * 语句
     */
    private byte[] stat;
    /**
     * 处理到的位置
     */
    private int pos;
    /**
     * 当前的token
     */
    private String currentToken;
    /**
     * 刷新token标志
     */
    private boolean flushToken;
    /**
     * 异常
     */
    private Exception err;

    /**
     * 构造函数
     * @param stat
     */
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        // flushToken = true
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 获取当前的token，如果flushToken为true的话，会生成新的token。
     * @return
     * @throws Exception
     */
    public String peek() throws Exception {
        // 有异常则抛出异常
        if(err != null) {
            throw err;
        }
        // 如果 flushToken==true，解析出下一个token
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false; // flushToken设置为false
        }
        return currentToken;
    }

    /**
     * 将当前的标记设置为需要刷新，这样下次调用peek()时会生成新的标记。
     */
    public void pop() {
        flushToken = true;
    }

    /**
     * err存在是，输出的Invalid statement
     * @return
     */
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    /**
     * 跳过该字节，指向下一个字节
     */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    /**
     * 获取下一个字节
     * @return
     */
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    /**
     * 获取下一个token。如果存在错误，将抛出异常。
     * @return
     * @throws Exception
     */
    private String next() throws Exception {
        // 如果存在错误，抛出异常
        if(err != null) {
            throw err;
        }
        // 否则，获取下一个元状态
        return nextMetaState();
    }

    /**
     * 获取下一个token。token可以是一个符号、引号包围的字符串或者一个由字母、数字或下划线组成字符串。
     * @return
     * @throws Exception
     */
    private String nextMetaState() throws Exception {
        // 找到不是空白字符的字节
        while(true) {
            Byte b = peekByte();     // 获取下一个字节
            if(b == null) {
                return "";  // 如果没有下一个字节，返回空字符串
            }
            if(!isBlank(b)) {
                break;  // 如果下一个字节不是空白字符，跳出循环
            }
            popByte();  // 否则，跳过这个字节
        }
        // 开始获取token
        byte b = peekByte();    // 获取下一个字节
        if(isSymbol(b)) {
            popByte();  // 如果这个字节是一个符号，跳过这个字节
            return new String(new byte[]{b});   // 并返回这个符号
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();    // 如果这个字节是引号，获取下一个引号状态
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();    // 如果这个字节是字母、数字或下划线，获取下一个标记状态
        } else {
            err = Error.InvalidCommandException;    // 否则，设置错误状态为无效的命令异常
            throw err;  // 并抛出异常
        }
    }

    /**
     * 获取下一个token，token是由字母、数字或下划线组成的字符串。
     * @return
     * @throws Exception
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder(); // 创建一个StringBuilder，用于存储标记
        while(true) {
            Byte b = peekByte();    // 获取下一个字节
            // 如果没有下一个字节，或者下一个字节不是字母、数字或下划线，那么结束循环
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                // 如果下一个字节是空白字符，那么跳过这个字节
                if(b != null && isBlank(b)) {
                    popByte();
                }
                // 返回标记
                return sb.toString();
            }
            // 如果下一个字节是字母、数字或下划线，那么将这个字节添加到StringBuilder中
            sb.append(new String(new byte[]{b}));
            // 跳过这个字节
            popByte();
        }
    }

    /**
     * byte是否是数字
     * @param b
     * @return
     */
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    /**
     * byte是否是字母
     * @param b
     * @return
     */
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * 获取token，即获取被引号包围的字符串。
     * @return
     * @throws Exception
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();    // 获取下一个字节，这应该是一个引号
        popByte();  // 跳过这个引号
        StringBuilder sb = new StringBuilder(); // 创建一个StringBuilder，用于存储被引号包围的字符串
        while(true) {
            Byte b = peekByte();    // 获取下一个字节
            if(b == null) {
                err = Error.InvalidCommandException;    // 如果没有下一个字节，设置错误状态为无效的命令异常
                throw err;  // 并抛出异常
            }
            if(b == quote) {
                popByte();  // 如果这个字节是引号，跳过这个字节，并跳出循环
                break;
            }
            sb.append(new String(new byte[]{b}));   // 如果这个字节不是引号，将这个字节添加到StringBuilder中
            popByte();  // 并跳过这个字节
        }
        return sb.toString();   // 返回被引号包围的字符串
    }

    /**
     * 判断字节是否是符号
     * @param b
     * @return
     */
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')');
    }

    /**
     * 判断字节是否为空、\t和\n。
     * @param b
     * @return
     */
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
