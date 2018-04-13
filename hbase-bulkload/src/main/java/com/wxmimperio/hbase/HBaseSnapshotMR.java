package com.wxmimperio.hbase;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import net.iharder.base64.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class HBaseSnapshotMR {

    private static final Log LOG = LogFactory.getLog(HBaseSnapshotMR.class);
    // Optional parameters for scanner
    final static String RAW_SCAN = "hbase.mapreduce.include.deleted.rows";
    final static String EXPORT_BATCHING = "hbase.export.scanner.batch";

    final static String START_TIME = "hbase.export.scanner.starttime";
    final static String STOP_TIME = "hbase.export.scanner.stoptime";
    final static String VERSION = "hbase.export.scanner.versions";
    final static String FILTER_CRITERIA = "hbase.export.scanner.filterCriteria";

    public static String TABLE_NAME = "tableName";
    public static String OUTPUT_PATH = "outputPath";

    public static String TMP_DIR = "tmpDir";
    public static String DEFAULT_TMP_DIR = "/wxm/tmp/dataExport";

    public static String SNAPSHOT_NAME = "snapshot";

    public static String FAMILY = "c";
    public static String DEFAULT_FAMILY = "";

    private String tableName;
    private String outputPath;
    private String tmpDir;
    private String snapshot;
    private Configuration conf;

    public void init(Properties props, Configuration conf, String[] args) throws IOException {
        this.snapshot = props.getProperty(SNAPSHOT_NAME);
        this.outputPath = props.getProperty(OUTPUT_PATH);
        this.conf = conf;
        this.tmpDir = props.getProperty(TMP_DIR, DEFAULT_TMP_DIR);
        this.tableName = props.getProperty(TABLE_NAME);
        for (Object key : props.keySet()) {
            if (key instanceof String) {
                String keyString = (String) key;
                this.conf.set(keyString, props.getProperty(keyString));
            }
        }
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.set(org.apache.hadoop.hbase.mapred.TableInputFormat.COLUMN_LIST,
                props.getProperty(FAMILY, DEFAULT_FAMILY).replace(',', ' '));
        conf.set(TableInputFormat.SCAN, convertScanToString(getConfiguredScanForJob(conf)));
        conf.setStrings("io.serializations", conf.get("io.serializations"),
                MutationSerialization.class.getName(),
                ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());
    }

    /**
     * Convert scan object into String.
     *
     * @param scan Scan.
     * @return String of the scan object.
     * @throws IOException
     */
    static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    /**
     * Construct scan from job configuration.
     *
     * @param conf Hadoop configuration.
     * @return Scan object.
     * @throws IOException
     */
    public static Scan getConfiguredScanForJob(Configuration conf) throws IOException {
        Scan s = new Scan();
        s.setBatch(5000);
        // Optional arguments.
        // Set Scan Versions
        int versions = conf.getInt(VERSION, 1);
        s.setMaxVersions(versions);
        // Set Scan Range
        long startTime = conf.getLong(START_TIME, 0L); //args.length > 3? Long.parseLong(args[3]): 0L;
        long endTime = conf.getLong(STOP_TIME, Long.MAX_VALUE); //rgs.length > 4? Long.parseLong(args[4]): Long.MAX_VALUE;
        s.setTimeRange(startTime, endTime);
        // Set cache blocks
        s.setCacheBlocks(false);
        // set Start and Stop row
        if (conf.get(TableInputFormat.SCAN_ROW_START) != null) {
            s.setStartRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_START)));
        }
        if (conf.get(TableInputFormat.SCAN_ROW_STOP) != null) {
            s.setStopRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_STOP)));
        }
        // Set Scan Column Family
        boolean raw = Boolean.parseBoolean(conf.get(RAW_SCAN));
        if (raw) {
            s.setRaw(raw);
        }

        if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
            s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
        }
        // Set RowFilter or Prefix Filter if applicable.
        /*Filter exportFilter = getExportFilter(conf);
        if (exportFilter != null) {
            LOG.info("Setting Scan Filter for Export.");
            s.setFilter(exportFilter);
        }*/

        // You may also specify all other filters through Scan interface.
        int batching = conf.getInt(EXPORT_BATCHING, -1);
        if (batching != -1) {
            try {
                s.setBatch(batching);
            } catch (IncompatibleFilterException e) {
                LOG.error("Batching could not be set", e);
            }
        }
        LOG.info("versions=" + versions + ", starttime=" + startTime +
                ", endtime=" + endTime + ", keepDeletedCells=" + raw);
        return s;
    }

    /*
     * Get Filter instance from given configuration.
     *
     * @param conf Hadoop configuration.
     * @return Filter instance.
     */
    /*private static Filter getExportFilter(Configuration conf) {
        Filter exportFilter = null;
        String filterCriteria = conf.get(FILTER_CRITERIA); //args.length > 5) ? args[5]: null;
        if (filterCriteria == null) return null;
        if (filterCriteria.startsWith("^")) {
            String regexPattern = filterCriteria.substring(1, filterCriteria.length());
            exportFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexPattern));
        } else {
            exportFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
        }
        return exportFilter;
    }*/

    public static class HbaseExportMapper extends Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result> {
        private Configuration conf;

        enum MyCounter {
            RECORDCOUNT
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            conf = context.getConfiguration();
        }
    }

    public void submit() {
        // SchemaMetrics.configureGlobally(conf);
        // prepareFiles();
        try {
            Job job = new Job(conf, "HBase Export");
            job.setJarByClass(HBaseSnapshotMR.class);
            job.setMapperClass(HbaseExportMapper.class);

            job.setInputFormatClass(TableSnapshotInputFormat.class);
            TableSnapshotInputFormat.setInput(job, snapshot, new Path(tmpDir));

            TableMapReduceUtil.addDependencyJars(job);
            job.setNumReduceTasks(0);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Result.class);

            job.waitForCompletion(true);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        // set the configuration back, so that Tool can configure itself
        args = parser.getRemainingArgs();

        if (args.length < 1) {
            LOG.error("Usage: <prperties-file>");
            System.exit(0);
        }

        HBaseSnapshotMR client = new HBaseSnapshotMR();
        Properties props = new Properties();
        props.load(new FileReader(args[0]));
        client.init(props, conf, args);
        client.submit();
    }
}
