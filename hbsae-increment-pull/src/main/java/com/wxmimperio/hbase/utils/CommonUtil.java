package com.wxmimperio.hbase.utils;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class CommonUtil {

    private static String STREAM_CHARSET = "ISO-8859-1";
    private static String ENCODER_CHARSET = "UTF-8";

    public static String writeToStr(Object obj) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(obj);
            return URLEncoder.encode(byteArrayOutputStream.toString(STREAM_CHARSET), ENCODER_CHARSET);
        }
    }

    public static Object deserializeFromStr(String serStr) throws IOException, ClassNotFoundException {
        String deserializeStr = URLDecoder.decode(serStr, ENCODER_CHARSET);
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(deserializeStr.getBytes(STREAM_CHARSET));
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            return objectInputStream.readObject();
        }
    }
}
