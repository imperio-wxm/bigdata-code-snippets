package com.wxmimperio.kudu.connection;

import com.wxmimperio.kudu.exception.KuduClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.KuduClient;

public class KuduConnection {

    private KuduClient client;
    private String masterIps;

    public KuduConnection(String masterIps) throws KuduClientException {
        this.masterIps = masterIps;
        initConnection();
    }

    private void initConnection() throws KuduClientException {
        if (StringUtils.isEmpty(masterIps)) {
            throw new KuduClientException("Master ips can not be empty.");
        }
        client = new KuduClient.KuduClientBuilder(masterIps).build();
    }

    public KuduClient getClient() {
        return client;
    }
}
