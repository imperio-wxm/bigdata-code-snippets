package com.wxmimperio.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class HFileInputFormat extends FileInputFormat<ImmutableBytesWritable, Cell> {

    @Override
    public RecordReader<ImmutableBytesWritable, Cell> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new HFileRecordReader();
    }

    private class HFileRecordReader extends RecordReader<ImmutableBytesWritable, Cell> {
        HFile.Reader reader;
        HFileScanner scanner;
        Integer entryNumber = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            Path path = ((FileSplit) split).getPath();
            FileSystem fs = org.apache.hadoop.fs.FileSystem.get(context.getConfiguration());
            reader = HFile.createReader(fs, path, new CacheConfig(context.getConfiguration()), context.getConfiguration());
            scanner = reader.getScanner(false, false);
            reader.loadFileInfo();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            entryNumber += 1;
            if (!scanner.isSeeked())
                // Had to move this here because "nextKeyValue" is called before the first getCurrentKey
                // which was causing us to miss the first row of the HFile.
                return scanner.seekTo();
            else {
                return scanner.next();
            }
        }

        @Override
        public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
            return new ImmutableBytesWritable(scanner.getKeyValue().getRow());
        }

        @Override
        public Cell getCurrentValue() throws IOException, InterruptedException {
            return scanner.getKeyValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (entryNumber / (float) reader.getEntries());
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
