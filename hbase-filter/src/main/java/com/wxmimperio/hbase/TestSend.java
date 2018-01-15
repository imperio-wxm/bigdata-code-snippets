package com.wxmimperio.hbase;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class TestSend {

    public static void main(String[] args) throws Exception {
        Socket s = null;
        PrintWriter pw = null;
        try {
            s = new Socket("10.128.74.82", 5240);
            pw = new PrintWriter(s.getOutputStream(), true);
            String content = "pt_newbee_guide_glog|2018-01-15 23:29:30|168168|001|1|123456789|ABCD999268EF9|10|G10|1|5" + "\n";
            pw.write(content);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pw.flush();
            pw.close();
            s.close();
        }
    }
}
