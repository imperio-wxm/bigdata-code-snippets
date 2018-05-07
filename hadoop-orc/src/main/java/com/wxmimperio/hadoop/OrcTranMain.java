package com.wxmimperio.hadoop;

import com.wxmimperio.hadoop.tran.SequenceToOrc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class OrcTranMain {
    private static final Logger LOG = LoggerFactory.getLogger(OrcTranMain.class);

    public static void main(String[] args) throws Exception {
        String sequenceFilePath = args[0];
        String outputPath = args[1];
        String dbName = args[2];
        String tableName = args[3];

        if (args.length != 4) {
            LOG.error("Params size error! Args = " + Arrays.asList(args));
        }

        long start = System.currentTimeMillis();

        SequenceToOrc.getSequenceWriter(sequenceFilePath, outputPath, dbName, tableName);

        LOG.info("Cost = " + (System.currentTimeMillis() - start) + " ms");
    }
}
