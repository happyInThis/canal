package com.alibaba.otter.canal.client.adapter.util;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zengzhangqiang on 05/11/2018.
 */
public class HttpClientUtils {
    private static final Logger HttpClientUtilsLog = LoggerFactory.getLogger("HttpClientUtilsLog");

    private static final int DEFAULT_POOL_MAX_TOTAL = 1024;
    private static final int DEFAULT_POOL_MAX_PER_ROUTE = 300;
    private static final int DEFAULT_CONNECT_TIMEOUT = 2000;
    private static final int DEFAULT_CONNECT_REQUEST_TIMEOUT = 1000;
    private static final int DEFAULT_SOCKET_TIMEOUT = 3000;

    private PoolingHttpClientConnectionManager gcm = null;
    private CloseableHttpClient httpClient = null;
    private IdleConnectionMonitorThread idleThread = null;

    // 连接池的最大连接数
    private final int maxTotal;
    // 连接池按route配置的最大连接数
    private final int maxPerRoute;
    // tcp connect的超时时间
    private final int connectTimeout;
    // 从连接池获取连接的超时时间
    private final int connectRequestTimeout;
    // tcp io的读写超时时间
    private final int socketTimeout;

    private HttpClientUtils() {
        this(HttpClientUtils.DEFAULT_POOL_MAX_TOTAL,
                HttpClientUtils.DEFAULT_POOL_MAX_PER_ROUTE,
                HttpClientUtils.DEFAULT_CONNECT_TIMEOUT,
                HttpClientUtils.DEFAULT_CONNECT_REQUEST_TIMEOUT,
                HttpClientUtils.DEFAULT_SOCKET_TIMEOUT);
    }

    private HttpClientUtils(int maxTotal, int maxPerRoute, int connectTimeout,
                            int connectRequestTimeout, int socketTimeout) {
        this.maxTotal = maxTotal;
        this.maxPerRoute = maxPerRoute;
        this.connectTimeout = connectTimeout;
        this.connectRequestTimeout = connectRequestTimeout;
        this.socketTimeout = socketTimeout;

        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", SSLConnectionSocketFactory.getSocketFactory())
                .build();

        this.gcm = new PoolingHttpClientConnectionManager(registry);
        this.gcm.setMaxTotal(this.maxTotal);
        this.gcm.setDefaultMaxPerRoute(this.maxPerRoute);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(this.connectTimeout)                     //设置连接超时
                .setSocketTimeout(this.socketTimeout)                       //设置读取超时
                .setConnectionRequestTimeout(this.connectRequestTimeout)    //设置从连接池获取连接实例的超时
                .build();

        HttpClientBuilder httpClientBuilder = HttpClients.custom();
        httpClient = httpClientBuilder
                .setConnectionManager(this.gcm)
                .setDefaultRequestConfig(requestConfig)
                .build();

        idleThread = new IdleConnectionMonitorThread(this.gcm);
        idleThread.start();

    }

    private static class HttpClientUtilsHolder {
        private final static HttpClientUtils instance = new HttpClientUtils();
        private final static HttpClientUtils fileUploaderInstance = new HttpClientUtils(100, 100, 10000, 10000, 10000);
    }

    public static HttpClientUtils getInstance() {
        return HttpClientUtilsHolder.instance;
    }

    public static HttpClientUtils getFileUploaderInstance() {
        return HttpClientUtilsHolder.fileUploaderInstance;
    }

    public String get(String url) {
        return this.get(url, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    public String get(String url, Map<String, String> params) {
        return this.get(url, Collections.EMPTY_MAP, params);
    }

    public String get(String url, Map<String, String> headers, Map<String, String> params) {

        //构建GET请求头
        String apiUrl = getUrlWithParams(url, params);
        HttpGet httpGet = new HttpGet(apiUrl);

        // *) 设置header信息
        if (headers != null && headers.size() > 0) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpGet.addHeader(entry.getKey(), entry.getValue());
            }
        }

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            if (response == null || response.getStatusLine() == null) {
                return null;
            }

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                HttpEntity entityRes = response.getEntity();
                if (entityRes != null) {
                    return EntityUtils.toString(entityRes, "UTF-8");
                }
            }
            return null;
        } catch (Exception e) {
            HttpClientUtilsLog.error("error to get, apiUrl:{}", apiUrl, e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                }
            }
        }
        return null;
    }

    public String postFile(String apiUrl, Map<String, String> params, File file) {
        CloseableHttpResponse response = null;
        try {
            HttpPost httpPost = new HttpPost(apiUrl);
            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
            // 把文件转换成流对象FileBody
            FileBody fileBody = new FileBody(file);
            //设置需要上传的文件
            multipartEntityBuilder.addPart("file", fileBody);//相当于<input type="file" name="file"/>

            //设置其他参数
            if (params != null && params.isEmpty() == false) {
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    ContentType contentType = ContentType.create("text/plain", Charset.forName("UTF-8"));
                    StringBody paramValue = new StringBody(entry.getValue(), contentType);
                    multipartEntityBuilder.addPart(entry.getKey(), paramValue);
                }
            }
            httpPost.setEntity(multipartEntityBuilder.build());
            response = httpClient.execute(httpPost);
            if (response == null || response.getStatusLine() == null) {
                return null;
            }

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                HttpEntity entityRes = response.getEntity();
                if (entityRes != null) {
                    return EntityUtils.toString(entityRes, "UTF-8");
                }
            }
            return null;
        } catch (Exception e) {
            HttpClientUtilsLog.error("error to post file, apiUrl:{}", apiUrl, e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignore) {
                }
            }
        }
        return null;
    }

    public String postWithData(String apiUrl, Map<String, String> params, byte[] postData) {
        return postWithData(apiUrl, null, params, postData);
    }

    public String postWithData(String apiUrl, List<Header> headerList, Map<String, String> params, byte[] postData) {
        HttpPost httpPost = new HttpPost(apiUrl);
        //配置请求参数
        if (params != null && params.size() > 0) {
            HttpEntity entityReq = getUrlEncodedFormEntity(params);
            httpPost.setEntity(entityReq);
        }

        //设置header
        if (headerList != null && headerList.isEmpty() == false) {
            for (Header header : headerList) {
                //只添加之前没有的header。content-length也忽略掉
                if (httpPost.containsHeader(header.getName())
                        || header.getName().equalsIgnoreCase("content-length")) {
                    continue;
                }
                httpPost.setHeader(header);
            }
        }

        //设置post数据
        if (postData != null && postData.length > 0) {
            InputStream inputStream = new ByteArrayInputStream(postData);
            httpPost.setEntity(new InputStreamEntity(inputStream, postData.length));
        }

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            if (response == null || response.getStatusLine() == null) {
                return null;
            }

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                HttpEntity entityRes = response.getEntity();
                if (entityRes != null) {
                    return EntityUtils.toString(entityRes, "UTF-8");
                }
            }
            return null;
        } catch (Exception e) {
            HttpClientUtilsLog.error("error to post with data, apiUrl:{}", apiUrl, e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignore) {
                }
            }
        }
        return null;
    }

    public String post(String apiUrl, Map<String, String> params) {
        return this.post(apiUrl, Collections.EMPTY_MAP, params);
    }

    public String post(String apiUrl, Map<String, String> headers, Map<String, String> params) {

        HttpPost httpPost = new HttpPost(apiUrl);
        //配置请求headers
        if (headers != null && headers.size() > 0) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpPost.addHeader(entry.getKey(), entry.getValue());
            }
        }

        //配置请求参数
        if (params != null && params.size() > 0) {
            HttpEntity entityReq = getUrlEncodedFormEntity(params);
            httpPost.setEntity(entityReq);
        }

        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            if (response == null || response.getStatusLine() == null) {
                return null;
            }

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                HttpEntity entityRes = response.getEntity();
                if (entityRes != null) {
                    return EntityUtils.toString(entityRes, "UTF-8");
                }
            }
            return null;
        } catch (Exception e) {
            HttpClientUtilsLog.error("error to post, apiUrl:{}", apiUrl, e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                }
            }
        }
        return null;

    }

    private HttpEntity getUrlEncodedFormEntity(Map<String, String> params) {
        List<NameValuePair> pairList = new ArrayList<NameValuePair>(params.size());
        for (Map.Entry<String, String> entry : params.entrySet()) {
            NameValuePair pair = new BasicNameValuePair(entry.getKey(), entry
                    .getValue().toString());
            pairList.add(pair);
        }
        return new UrlEncodedFormEntity(pairList, Charset.forName("UTF-8"));
    }

    private String getUrlWithParams(String url, Map<String, String> params) {
        boolean first = true;
        StringBuilder sb = new StringBuilder(url);
        for (String key : params.keySet()) {
            char ch = '&';
            if (first == true) {
                ch = '?';
                first = false;
            }
            String value = params.get(key).toString();
            try {
                String sval = URLEncoder.encode(value, "UTF-8");
                sb.append(ch).append(key).append("=").append(sval);
            } catch (UnsupportedEncodingException e) {
            }
        }
        return sb.toString();
    }


    public void shutdown() {
        idleThread.shutdown();
    }

    // 监控有异常的链接
    private class IdleConnectionMonitorThread extends Thread {

        private final HttpClientConnectionManager connMgr;
        private volatile boolean exitFlag = false;

        public IdleConnectionMonitorThread(HttpClientConnectionManager connMgr) {
            this.connMgr = connMgr;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (!this.exitFlag) {
                synchronized (this) {
                    try {
                        this.wait(3000);
                    } catch (InterruptedException e) {
                        HttpClientUtilsLog.error("error to wait", e);
                    }
                }
                // 关闭失效的连接
                connMgr.closeExpiredConnections();
                // 可选的, 关闭30秒内不活动的连接
                connMgr.closeIdleConnections(30, TimeUnit.SECONDS);
            }
        }

        public void shutdown() {
            this.exitFlag = true;
            synchronized (this) {
                notify();
            }
        }
    }

    public static void main(String[] args) {
        final HttpClientUtils client = new HttpClientUtils();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(100, 100, 30, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(1000));
        try {
            long currentTime = System.currentTimeMillis();
            final AtomicLong counter = new AtomicLong(300000);
            System.out.println("start");
            for (int i = 0; i < 100; i++) {
                threadPoolExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < 3000; i++) {
                            String resp = client.get("http://www.baidu.com");
                            counter.decrementAndGet();
                        }
                    }
                });
            }

            while (counter.get() > 0) {
                System.out.println("counter=" + counter.get() + ", cost:" + (System.currentTimeMillis() - currentTime));
                Thread.sleep(100);
            }
            System.out.println("cost:" + (System.currentTimeMillis() - currentTime));
            System.out.println("over");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
