/*
package com.wxmimperio.hadoop.reader;

import com.google.common.collect.Lists;
import com.wxmimperio.hadoop.utils.FileSystemUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class OrcFileReader implements Iterable<String> {
    private static final Logger LOG = LoggerFactory.getLogger(OrcFileReader.class);

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static final String COL_NULL = "";
    private RecordReader recordReader = null;
    private VectorizedRowBatch batch = null;
    private List<TypeDescription> flatSchema = null;

    public OrcFileReader(String uri) throws IOException {
        Reader reader = OrcFile.createReader(new Path(uri), OrcFile.readerOptions(FileSystemUtil.getConf()));
        recordReader = reader.rows();
        TypeDescription schema = reader.getSchema();
        batch = schema.createRowBatch(10);
        flatSchema = schema.getChildren();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
    }

    private Long readLong(ColumnVector cv, int index) {
        if (cv.isRepeating) {
            index = 0;
        }
        if (cv.noNulls || !cv.isNull[index]) {
            return ((LongColumnVector) cv).vector[index];
        } else {
            return null;
        }
    }

    private Double readDouble(ColumnVector cv, int index) {
        if (cv.isRepeating) {
            index = 0;
        }
        if (cv.noNulls || !cv.isNull[index]) {
            return ((DoubleColumnVector) cv).vector[index];
        } else {
            return null;
        }
    }

    private String readString(ColumnVector cv, int index) {
        if (cv.isRepeating) {
            index = 0;
        }
        if (cv.noNulls || !cv.isNull[index]) {
            return new String(((BytesColumnVector) cv).vector[index], DEFAULT_CHARSET);
        } else {
            return null;
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            private List<String> lineBuffer = new ArrayList<>();
            private int read = 0;

            */
/**
             * Get ColumnVector by \t
             * @param cvs
             * @param index
             * @return
             *//*

            private String getLine(ColumnVector[] cvs, int index) {
                List<String> lineArray = Lists.newArrayList();
                for (int i = 0; i < cvs.length; i++) {
                    lineArray.add(parseColumn(flatSchema.get(i), cvs[i], index));
                }
                String line = StringUtils.join(lineArray, "\t");
                System.out.println(line);
                return line;
            }

            */
/**
             * BOOLEAN("boolean", true), *
             * BYTE("tinyint", true),    *
             * SHORT("smallint", true),  *
             * INT("int", true),         *
             * LONG("bigint", true),     *
             * FLOAT("float", true),     *
             * DOUBLE("double", true),   *
             * STRING("string", true),   *
             * DATE("date", true),       X
             * TIMESTAMP("timestamp", true),
             * BINARY("binary", true),   *
             * DECIMAL("decimal", true), X
             * VARCHAR("varchar", true), *
             * CHAR("char", true),       *
             * LIST("array", false),     X
             * MAP("map", false),        X
             * STRUCT("struct", false),  X
             * UNION("uniontype", false);X
             *
             * @param td
             * @param cv
             * @param index
             * @return
             *//*

            private String parseColumn(TypeDescription td, ColumnVector cv, int index) {
                String result = null;
                switch (td.getCategory()) {
                    case INT:
                    case BYTE:
                    case SHORT:
                    case LONG:
                        Long longType = readLong(cv, index);
                        result = (longType == null ? COL_NULL : String.valueOf(longType));
                        break;
                    case FLOAT:
                    case DOUBLE:
                        Double doubleType = readDouble(cv, index);
                        result = (doubleType == null ? COL_NULL : String.valueOf(doubleType));
                        break;
                    case STRING:
                    case CHAR:
                    case VARCHAR:
                    case BINARY:
                        String stringType = readString(cv, index);
                        result = (stringType == null ? COL_NULL : stringType);
                        break;
                    default:
                        throw new UnsupportedOperationException(td.getCategory().getName());
                }
                return result;
            }

            private void reset() {
                read = 0;
                lineBuffer = null;
            }

            @Override
            public boolean hasNext() {
                try {
                    if (read >= lineBuffer.size()) {
                        // reset lineBuffer to start
                        reset();
                        if (recordReader.nextBatch(batch)) {
                            lineBuffer = Lists.newArrayList();
                            for (int i = 0; i < batch.size; i++) {
                                lineBuffer.add(getLine(batch.cols, i));
                            }
                            return true;
                        } else {
                            close();
                            return false;
                        }
                    } else {
                        return true;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public String next() {
                return lineBuffer.get(read++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void close() {
        if (recordReader != null) {
            try {
                recordReader.close();
            } catch (IOException e) {
                LOG.info("Close record reader error!", e);
            } finally {
                batch = null;
                recordReader = null;
            }
        }
    }
}
*/
