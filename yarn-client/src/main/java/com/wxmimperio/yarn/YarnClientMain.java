package com.wxmimperio.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.*;

public class YarnClientMain extends Configured {

    public static void main(String[] args) throws Exception {
        String appIdStr = args[0];
        String containerIdStr = null;
        String nodeAddress = null;
        String appOwner = null;

        Configuration configuration = new Configuration(false);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));

        YarnClientMain yarnClientMain = new YarnClientMain();
        yarnClientMain.setConf(configuration);
        yarnClientMain.getLogs(appIdStr, containerIdStr, nodeAddress, appOwner);
    }

    private int getLogs(String appIdStr, String containerIdStr, String nodeAddress, String appOwner) throws IOException {
        ApplicationId appId = null;
        try {
            appId = ConverterUtils.toApplicationId(appIdStr);
        } catch (Exception e) {
            System.err.println("Invalid ApplicationId specified");
            return -1;
        }

        try {
            int resultCode = verifyApplicationState(appId);
            if (resultCode != 0) {
                System.out.println("Logs are not avaiable right now.");
                return resultCode;
            }
        } catch (Exception e) {
            System.err.println("Unable to get ApplicationState."
                    + " Attempting to fetch logs directly from the filesystem.");
        }
        if (appOwner == null || appOwner.isEmpty()) {
            appOwner = UserGroupInformation.getCurrentUser().getShortUserName();
        }

        LogCLIHelpers logCliHelper = new LogCLIHelpers();
        logCliHelper.setConf(getConf());

        int resultCode = 0;
        if (containerIdStr == null && nodeAddress == null) {
            File file = new File("/home/hadoop/wxm/spark/test.txt");
            PrintStream out = new PrintStream(new FileOutputStream(file, true));
            resultCode = logCliHelper.dumpAllContainersLogs(appId, appOwner, out);
        } else if ((containerIdStr == null && nodeAddress != null)
                || (containerIdStr != null && nodeAddress == null)) {
            System.out.println("ContainerId or NodeAddress cannot be null!");
            //printHelpMessage(printOpts);
            resultCode = -1;
        } else {
            resultCode = logCliHelper.dumpAContainersLogs(appIdStr, containerIdStr,
                    nodeAddress, appOwner);
        }

        return resultCode;

    }

    private int verifyApplicationState(ApplicationId appId) throws IOException, YarnException {
        YarnClient yarnClient = createYarnClient();

        try {
            ApplicationReport appReport = yarnClient.getApplicationReport(appId);
            switch (appReport.getYarnApplicationState()) {
                case NEW:
                case NEW_SAVING:
                case SUBMITTED:
                    return -1;
                case ACCEPTED:
                case RUNNING:
                case FAILED:
                case FINISHED:
                case KILLED:
                default:
                    break;

            }
        } finally {
            yarnClient.close();
        }
        return 0;
    }

    protected YarnClient createYarnClient() {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(getConf());
        yarnClient.start();
        return yarnClient;
    }
}
