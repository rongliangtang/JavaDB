package cn.tangrl.javadb.backend.parser.statement;

/**
 * 简单的表达式对象
 * 例如：id > 5
 */
public class SingleExpression {
    public String field;
    public String compareOp;
    public String value;
}
