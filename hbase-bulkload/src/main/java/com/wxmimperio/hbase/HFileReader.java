package com.wxmimperio.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HFileReader {

    private static String HBASE_SITE = "hbaes-site.xml";
    private static HFile.Reader reader;
    private static HFileScanner scanner;
    private static Configuration conf;

    public static void main(String[] args) throws IOException {
        Path path = new Path(args[0]);
        conf = HBaseConfiguration.create();
        conf.addResource(HBASE_SITE);
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        reader = HFile.createReader(FileSystem.get(conf), path, new CacheConfig(conf), conf);

        scanner = reader.getScanner(false, false);
        reader.loadFileInfo();
        scanner.seekTo();

        while (scanner.next()) {
            Cell cell = scanner.getKeyValue();
            System.out.println("rowkey = " + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("cf = " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("column value = " + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("column name = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("===========");
        }
    }
}
