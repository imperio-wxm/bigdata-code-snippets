package com.wxmimperio;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.wxmimperio.auth.auth.FlumeAuthenticationUtil;
import com.wxmimperio.auth.auth.PrivilegedExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AuthMain {

    public static void main(String[] args) throws Exception {
        ExecutorService callTimeoutPool = Executors.newFixedThreadPool(
                10,
                new ThreadFactoryBuilder().setNameFormat("thread pool").build()
        );

        String kerbConfPrincipal = "";
        String kerbKeytab = "";
        String proxyUser = "";
        PrivilegedExecutor privExecutor = FlumeAuthenticationUtil.getAuthenticator(
                kerbConfPrincipal,
                kerbKeytab
        ).proxyAs(proxyUser);

        Configuration config = new Configuration();
        String bucketPath = "";

        FileSystem fs = new Path(bucketPath).getFileSystem(config);

        String srcPath = "";

        Future<Boolean> future = callTimeoutPool.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return privExecutor.execute(new PrivilegedExceptionAction<Boolean>() {
                    @Override
                    public Boolean run() throws Exception {
                        return fs.exists(new Path(srcPath));
                    }
                });
            }
        });
        future.get();
    }
}
