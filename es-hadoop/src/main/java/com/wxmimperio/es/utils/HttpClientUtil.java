package com.wxmimperio.es.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class HttpClientUtil {

    private static HttpClient httpClient;

    private static HttpClient getHttpClient() {
        httpClient = new DefaultHttpClient();
        return httpClient;
    }

    public static String doGet(String url) throws IOException {
        HttpClient client = getHttpClient();
        HttpGet httpGet = new HttpGet(url);
        try {
            HttpResponse response = client.execute(httpGet);
            validateStatus(response);
            HttpEntity entity = response.getEntity();
            return getContent(entity);
        } finally {
            close();
        }
    }

    private static void validateStatus(HttpResponse response) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status < 200 || status >= 300) {
            throw new IOException("Unexpected response status: " + status);
        }
    }

    private static void close() {
        if (httpClient != null) {
            httpClient.getConnectionManager().shutdown();
        }
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
