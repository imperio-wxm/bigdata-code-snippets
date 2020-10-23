package com.wxmimperio.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RocksDb.java
 * @description This is the description of RocksDb.java
 * @createTime 2020-10-23 11:28:00
 */
public class RocksDb {
    private static final String dbPath = "rocksdb";
    private RocksDB rocksDB;

    static {
        RocksDB.loadLibrary();
    }

    /**
     * https://houbb.github.io/2018/09/06/cache-rocksdb
     * https://www.orchome.com/938
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        RocksDb rocksDb = new RocksDb();
        rocksDb.testDefaultColumnFamily();
    }

    public void testDefaultColumnFamily() throws RocksDBException, IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        // 文件不存在，则先创建文件
        if (!Files.isSymbolicLink(Paths.get(dbPath))) {
            Files.createDirectories(Paths.get(dbPath));
        }
        rocksDB = RocksDB.open(options, dbPath);

        /**
         * 简单key-value
         */
        byte[] key = "Hello".getBytes();
        byte[] value = "World".getBytes();
        rocksDB.put(key, value);

        byte[] getValue = rocksDB.get(key);
        System.out.println(new String(getValue));


        /**
         * 通过List做主键查询
         * 一次查询多个key的数据
         */
        rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key);
        keys.add("SecondKey".getBytes());

        // 废弃，用multiGetAsList替代
        Map<byte[], byte[]> valueMap = rocksDB.multiGet(keys);
        for (Map.Entry<byte[], byte[]> entry : valueMap.entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }

        System.out.println("=====");
        List<byte[]> valueList = rocksDB.multiGetAsList(keys);
        valueList.forEach(v -> {
            System.out.println(new String(v));
        });


        rocksDB.put("wxm_key".getBytes(), "wxm_value".getBytes());
        /**
         *  打印全部[key - value]
         */
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

        /**
         *  删除一个key
         */
        rocksDB.delete(key);
        System.out.println("after remove key:" + new String(key));

        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }
    }
}
