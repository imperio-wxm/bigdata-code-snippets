package com.wxmimperio.kudu.utils;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HttpClientUtil {
    public static String doPost(String url, String body, Header[] headers) throws IOException {
        return doPostCommon(url, new StringEntity(body), headers);
    }

    public static String doPost(String url, Map<String, String> param, Header[] headers) throws IOException {
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        for (Entry<String, String> entry : param.entrySet()) {
            nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        HttpEntity httpEntity = new UrlEncodedFormEntity(nvps);
        return doPostCommon(url, httpEntity, headers);
    }

    private static String doPostCommon(String url, HttpEntity httpEntity, Header[] headers) throws IOException {
        CloseableHttpClient client = getHttpClient();
        HttpPost httpPost = new HttpPost(url);
        httpPost.setEntity(httpEntity);
        if (headers != null) {
            httpPost.setHeaders(headers);
        }
        try (CloseableHttpResponse response = client.execute(httpPost)) {
            validateStatus(response);
            HttpEntity responseEntity = response.getEntity();
            return getContent(responseEntity);
        }
    }

    public static String doGet(String url) throws IOException {
        CloseableHttpClient client = getHttpClient();
        HttpGet httpGet = new HttpGet(url);
        try (CloseableHttpResponse response = client.execute(httpGet)) {
            validateStatus(response);
            HttpEntity entity = response.getEntity();
            return getContent(entity);
        }
    }

    private static void validateStatus(HttpResponse response) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status < 200 || status >= 300) {
            throw new IOException("Unexpected response status: " + status);
        }
    }

    private static CloseableHttpClient getHttpClient() {
        return HttpClients.createDefault();
    }

    private static String getContent(HttpEntity entity) throws IOException {
        InputStream is = entity.getContent();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int i = -1;
        while ((i = is.read()) != -1) {
            baos.write(i);
        }
        is.close();
        baos.flush();
        baos.close();
        return baos.toString();
    }
}
