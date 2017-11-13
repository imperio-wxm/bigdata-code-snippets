package com.wxmimperio.hadoop.reader;

import com.wxmimperio.hadoop.utils.FileSystemUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;


public class OrcFileReader implements Iterable<String> {
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static final String COL_NULL_SHOW = "";
    private RecordReader recordReader = null;
    private VectorizedRowBatch batch = null;
    private List<TypeDescription> flatSchema = null;

    public OrcFileReader(String uri) throws IOException {
        Reader reader = OrcFile.createReader(new Path(uri), OrcFile.readerOptions(FileSystemUtil.getConf()));
        recordReader = reader.rows();
        TypeDescription schema = reader.getSchema();
        batch = schema.createRowBatch();
        flatSchema = schema.getChildren();
    }

    @Override
    public Iterator<String> iterator() {
        return null;
    }
}
