package com.wxmimperio.phoenix;

import com.wxmimperio.phoenix.ops.PhoenixDDL;

public class PhoenixMain {

    public static void main(String[] args) throws Exception {
        PhoenixDDL phoenixDDL = new PhoenixDDL();
        phoenixDDL.createTable("");

        phoenixDDL.closeDataSource();
    }
}
