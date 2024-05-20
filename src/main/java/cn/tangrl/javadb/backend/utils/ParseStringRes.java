package cn.tangrl.javadb.backend.utils;

/**
 * 包裹字符串对象
 *
 * 段和表的字段等信息都是字节形式的字符串。这里规定一个字符串的存储方式，以明确其存储边界。
 * [StringLength][StringData]
 * 这个类将这个字节数组包裹成这个对象。
 */
public class ParseStringRes {
    /**
     * 实际的字符串数据
     */
    public String str;
    /**
     * 包裹数组的长度，下一个包裹数组也是这个长度。
     */
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}
