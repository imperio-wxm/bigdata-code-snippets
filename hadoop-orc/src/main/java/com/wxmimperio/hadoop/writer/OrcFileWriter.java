package com.wxmimperio.hadoop.writer;

import com.wxmimperio.hadoop.utils.FileSystemUtil;
import com.wxmimperio.hadoop.utils.OrcUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class OrcFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(OrcFileWriter.class);

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static String DEFAULT_COL_SPLITTER = "\t";

    public static long writeOrc(String db, String table, List<String> buffer, String hdfsDesPath, int writeBatch) throws Exception {
        TypeDescription schema = OrcUtils.getColumnTypeDescs(db, table);
        long totalLine = 0L;
        try (Writer writer = OrcFile.createWriter(new Path(hdfsDesPath), OrcFile.writerOptions(FileSystemUtil.getConf()).setSchema(schema).compress(CompressionKind.SNAPPY))) {
            VectorizedRowBatch batch = schema.createRowBatch(writeBatch);
            String[] cols;
            for (String line : buffer) {
                totalLine++;
                cols = line.split(DEFAULT_COL_SPLITTER, -1);
                if (cols.length != schema.getFieldNames().size()) {
                    LOG.error("Write orc error, data = " + line);
                } else {
                    for (int i = 0; i < schema.getFieldNames().size(); i++) {
                        setColumnVectorVal(schema.getChildren().get(i), batch.cols[i], batch.size, cols[i]);
                    }
                    batch.size++;
                    if (batch.size == batch.getMaxSize()) {
                        try {
                            writer.addRowBatch(batch);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            batch.reset();
                        }
                    }
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            LOG.info("finish write orc file for " + hdfsDesPath);
        }
        return totalLine;
    }

    public static Writer getOrcWriter(String hdfsDesPath, TypeDescription schema) throws Exception {
        Writer writer = OrcFile.createWriter(
                new Path(hdfsDesPath),
                OrcFile.writerOptions(FileSystemUtil.getConf()).setSchema(schema).compress(CompressionKind.SNAPPY)
        );
        return writer;
    }

    public static void writeData(Writer writer, TypeDescription schema, List<String> buffer) throws Exception {
        VectorizedRowBatch batch = schema.createRowBatch(10000);
        String[] cols;
        for (String line : buffer) {
            cols = line.split(DEFAULT_COL_SPLITTER, -1);
            if (cols.length != schema.getFieldNames().size()) {
                LOG.error("Write orc error, data = " + line);
            } else {
                for (int i = 0; i < schema.getFieldNames().size(); i++) {
                    setColumnVectorVal(schema.getChildren().get(i), batch.cols[i], batch.size, cols[i].trim());
                }
                // 行数，最大为createRowBatch—> maxSize
                batch.size++;
                if (batch.size == batch.getMaxSize()) {
                    try {
                        writer.addRowBatch(batch);
                    } catch (Exception e) {
                        LOG.error("Add data error! data = " + Arrays.asList(cols) + ", writer = " + writer + ", batch size = " + batch.size, e);
                    } finally {
                        batch.reset();
                    }
                }
            }
        }
        if (batch.size > 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    private static void setColumnVectorVal(TypeDescription td, ColumnVector cv, int rc, String val) {
        if (null == val || "".equals(val) || "\\N".equals(val)) {
            cv.noNulls = false;
            cv.isNull[rc] = true;
        } else {
            try {
                switch (td.getCategory()) {
                    case BOOLEAN:
                        long bval = Long.parseLong(val) > 0 ? 1 : 0;
                        ((LongColumnVector) cv).vector[rc] = bval;
                        break;
                    case INT:
                    case LONG:
                    case BYTE:
                        ((LongColumnVector) cv).vector[rc] = Long.parseLong(val);
                        break;
                    case FLOAT:
                    case DOUBLE:
                        ((DoubleColumnVector) cv).vector[rc] = Double.parseDouble(val);
                        break;
                    case STRING:
                    case VARCHAR:
                    case BINARY:
                        //((BytesColumnVector) cv).setVal(rc, val.getBytes(DEFAULT_CHARSET));
                        break;
                    default:
                        throw new UnsupportedOperationException(td.getCategory() + ":" + val);
                }
            } catch (NumberFormatException e) {
                cv.noNulls = false;
                cv.isNull[rc] = true;
            }
        }
    }
}
