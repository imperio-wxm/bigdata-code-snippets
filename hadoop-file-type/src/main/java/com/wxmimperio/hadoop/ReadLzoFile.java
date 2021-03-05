package com.wxmimperio.hadoop;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className ReadLzoFile.java
 * @description This is the description of ReadLzoFile.java
 * @createTime 2021-01-27 17:07:00
 */
public class ReadLzoFile {
    private final static String ORC = ".orc";
    private final static String LZO = ".lzo";

    public static void main(String[] args) throws Exception {

        String delimiter = "\t";

        String localPath = args[0];
        File[] localFiles = new File(localPath).listFiles();
        for (File localFile : localFiles) {
            if (localFile.isHidden()) {
                continue;
            }
            try (InputStream inputStream = wrapInputStream(new FileInputStream(localFile), localFile.getName());
                 BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, delimiter);
                    System.out.println(Arrays.toString(fields));
                }
            }
        }
    }

    private static InputStream wrapInputStream(InputStream origin, String fileName) throws IOException {
        if (StringUtils.endsWithIgnoreCase(fileName, LZO)) {
            LzopCodec codec = new LzopCodec();
            codec.setConf(new Configuration());
            return codec.createInputStream(origin);
        }
        if (StringUtils.endsWithIgnoreCase(fileName, ORC)) {
            // todo
            return origin;
        }
        return origin;
    }
}
