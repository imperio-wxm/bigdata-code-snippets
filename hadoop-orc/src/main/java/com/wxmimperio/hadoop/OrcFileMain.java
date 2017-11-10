package com.wxmimperio.hadoop;

import com.wxmimperio.hadoop.utils.OrcUtils;


public class OrcFileMain {

    public static void main(String[] args) throws Exception {
        System.out.println(OrcUtils.getColumnTypeDescs("dw", "cd_item_glog"));
    }
}
