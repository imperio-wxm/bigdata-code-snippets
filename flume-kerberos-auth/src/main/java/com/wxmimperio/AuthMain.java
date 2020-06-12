package com.wxmimperio;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.wxmimperio.auth.auth.FlumeAuthenticationUtil;
import com.wxmimperio.auth.auth.PrivilegedExecutor;
import com.wxmimperio.common.ParamConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AuthMain {

    private static final Logger LOG = LoggerFactory.getLogger(AuthMain.class);


    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.out.println("error");
            System.exit(1);
        }
        ExecutorService callTimeoutPool = Executors.newFixedThreadPool(
                10,
                new ThreadFactoryBuilder().setNameFormat("thread pool").build()
        );

        PrivilegedExecutor privExecutor = FlumeAuthenticationUtil.getAuthenticator(
                ParamConstants.KERB_CONFIG_PRINCIPAL,
                ParamConstants.KERB_KEY_TAB
        ).proxyAs(ParamConstants.PROXY_USER);

        Configuration config = new Configuration();
        config.addResource(new Path(ParamConstants.CORE_SITE));
        config.addResource(new Path(ParamConstants.HDFS_SITE));
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        String bucketPath = "/flume/test/test3.xml";
        FileSystem fs = new Path(bucketPath).getFileSystem(config);

        String srcPath = args[0];
        String localPath = args[1];
        String hdfsPath = args[2];

        Future<Boolean> future = callTimeoutPool.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return privExecutor.execute(new PrivilegedExceptionAction<Boolean>() {
                    @Override
                    public Boolean run() throws Exception {
                        writeHDFS(fs, localPath, hdfsPath);
                        return fs.exists(new Path(srcPath));
                    }
                });
            }
        });
        LOG.info("Future result = " + future.get());
        callTimeoutPool.shutdown();
    }

    private static void writeHDFS(FileSystem fs, String localPath, String hdfsPath) {
        if (null == fs) {
            LOG.info("Fs is null!");
            return;
        }
        try (FSDataOutputStream outputStream = fs.create(new Path(hdfsPath));
             FileInputStream fileInputStream = new FileInputStream(new File(localPath))) {
            //输入流、输出流、缓冲区大小、是否关闭数据流，如果为false就在 finally里关闭
            IOUtils.copyBytes(fileInputStream, outputStream, 4096, true);
            LOG.info("Finish write to path = " + hdfsPath);
        } catch (IOException e) {
            LOG.error("Can not copy files, ", e);
        }
    }
}
