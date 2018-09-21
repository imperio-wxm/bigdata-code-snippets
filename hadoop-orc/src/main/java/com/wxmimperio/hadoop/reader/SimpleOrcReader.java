/*
package com.wxmimperio.hadoop.reader;


import com.google.common.collect.Lists;
import com.wxmimperio.hadoop.utils.FileSystemUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.orc.TypeDescription;

import java.nio.charset.Charset;
import java.util.List;

public class SimpleOrcReader {

    public static void main(String[] args) throws Exception {
        Path path = new Path(args[0]);
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(FileSystemUtil.getConf()));
        StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
        RecordReader recordReader = reader.rows();
        Object row = null;
        int index = 0;
        while (recordReader.hasNext()) {
            row = recordReader.next(row);
            List<Object> values = inspector.getStructFieldsDataAsList(row);
            StringBuilder builder = new StringBuilder();
            if (index++ >= 10) {
                break;
            }
            for (Object field : values) {
                builder.append(field);
                builder.append('|');
            }
            System.out.println(builder.toString());
        }
    }

    private static Object[] getLine(ColumnVector[] cvs, List<TypeDescription> flatSchema, int index) {
        List<Object> lineArray = Lists.newArrayList();
        for (int i = 0; i < cvs.length; i++) {
            lineArray.add(parseColumn(flatSchema.get(i), cvs[i], index));
        }
        return lineArray.toArray();
    }

    private static Object parseColumn(TypeDescription td, ColumnVector cv, int index) {
        switch (td.getCategory()) {
            case INT:
            case BYTE:
            case SHORT:
            case LONG:
                return readLong(cv, index);
            case FLOAT:
            case DOUBLE:
                return readDouble(cv, index);
            case STRING:
            case CHAR:
            case VARCHAR:
            case BINARY:
                return readString(cv, index);
            default:
                throw new UnsupportedOperationException(td.getCategory().getName());
        }
    }

    private static Long readLong(ColumnVector cv, int index) {
        */
/*if (cv.isRepeating) {
            index = 0;
        }*//*

        if (cv.noNulls || !cv.isNull[index]) {
            return ((LongColumnVector) cv).vector[index];
        } else {
            return null;
        }
    }

    private static Double readDouble(ColumnVector cv, int index) {
        */
/*if (cv.isRepeating) {
            index = 0;
        }*//*

        if (cv.noNulls || !cv.isNull[index]) {
            return ((DoubleColumnVector) cv).vector[index];
        } else {
            return null;
        }
    }

    private static String readString(ColumnVector cv, int index) {
        */
/*if (cv.isRepeating) {
            index = 0;
        }*//*

        if (cv.noNulls || !cv.isNull[index]) {
            return new String(((BytesColumnVector) cv).vector[index], Charset.forName("UTF-8"));
        } else {
            return null;
        }
    }
}
*/
