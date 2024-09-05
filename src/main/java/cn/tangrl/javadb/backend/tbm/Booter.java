package cn.tangrl.javadb.backend.tbm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.common.Error;

/**
 * Booter类
 * 这个文件只存放了第一个表的uid
 *
 * 由于 TBM 的表管理，使用的是链表串起的 Table 结构，所以就必须保存一个链表的头节点，即第一个表的 UID，这样在启动时，才能快速找到表信息。
 * 使用 Booter 类和 bt 文件，来管理数据库的启动信息。
 * 虽然现在所需的启动信息，只有一个：头表的 UID。
 * Booter 类对外提供了两个方法：load 和 update，并保证了其原子性。
 * update 在修改 bt 文件内容时，没有直接对 bt 文件进行修改，而是首先将内容写入一个 bt_tmp 文件中，随后将这个文件重命名为 bt 文件。
 * 通过操作系统重命名文件的原子性，来保证操作的原子性。
 */
public class Booter {
    /**
     * 文件后缀
     */
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";
    /**
     * 文件路径
     */
    String path;
    /**
     * 文件对象
     */
    File file;

    /**
     * 创建文件和Booter对象
     * @param path
     * @return
     */
    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /**
     * 打开文件，返回booter对象
     * @param path
     * @return
     */
    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /**
     * 删除.bt_tmp文件
     * @param path
     */
    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    /**
     * 构造方法
     * @param path
     * @param file
     */
    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    /**
     * 读取 file 对象的所有字节，即读取文件内容
     * @return
     */
    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     * update 在修改 bt 文件内容时，没有直接对 bt 文件进行修改，而是首先将内容写入一个 bt_tmp 文件中，随后将这个文件重命名为 bt 文件。
     * 通过操作系统重命名文件的原子性，来保证操作的原子性。
     * @param data
     */
    public void update(byte[] data) {
        // 创建.bt_tmp文件
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        // 将data写入到.bt_tmp文件中
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 将.bt_tmp重命名为.bt文件
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 判断这个文件能不能读写
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }

}
