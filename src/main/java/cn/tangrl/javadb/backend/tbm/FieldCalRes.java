package cn.tangrl.javadb.backend.tbm;

/**
 * 这个对象存放SingleExpression进一步解析的结果，存放比较表达式
 * = 操作时，left=rigt
 * < 操作时，left=0，right=value
 */
public class FieldCalRes {
    public long left;
    public long right;
}
