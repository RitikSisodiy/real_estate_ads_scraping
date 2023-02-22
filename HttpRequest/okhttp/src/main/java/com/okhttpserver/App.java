package com.okhttpserver;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import okio.Buffer;
import okio.GzipSource;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.io.OutputStream;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Base64;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.Request.Builder;
import java.net.InetSocketAddress;
import java.net.Proxy;
/**
 * Hello world!
 *
 */
public class App 
{
    // public static OkHttpClient client = new OkHttpClient();
    public static OkHttpClient client = new OkHttpClient();
    static final Logger logger = Logger.getLogger("Server");
    public static void main( String[] args ) throws IOException
    {
      HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8001), 0);

        server.createContext("/makerequest", new App.MyHttpHandler());
        server.createContext("/seloger-auth", new TokenServer.MyHttpHandler());
        server.setExecutor(Executors.newFixedThreadPool(3));
        server.start();
        logger.info(" Server started on port 8001");
    }
    public static JSONObject getRequestBodyJson(HttpExchange exchange) throws IOException, ParseException {
        InputStreamReader isr =  new InputStreamReader(exchange.getRequestBody(),"utf-8");
        BufferedReader br = new BufferedReader(isr);
        int b;
        StringBuilder buf = new StringBuilder(512);
        while ((b = br.read()) != -1) {
            buf.append((char) b);
        }
        br.close();
        isr.close();    
        JSONParser parser = new JSONParser(); 
        JSONObject json = (JSONObject) parser.parse(buf.toString());
        return json;
    }
    public static class MyHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            OutputStream stream = exchange.getResponseBody();
            try {
                // String decode = URLDecoder.decode(exchange.getRequestURI().getRawQuery(), "UTF-8");
                // JSONObject body = App.getRequestBodyJson(exchange);
                // System.out.println(body);
                // String decode = exchange.getRequestURI().getRawQuery();
                // Map<String, String> args = new HashMap<>();
                // for (String kv : decode.split("&")) {
                //     String[] kvArr = kv.split("=");
                //     args.put(kvArr[0], kvArr[1]);
                // }
                // Gson gson = new Gson(); 
                // String compact = gson.toJson(args); 
                byte[] bs = App.request(exchange);
                // byte[] bs = compact.getBytes("UTF-8");
                // System.out.println(bs);
                stream.write(bs);
            } catch (Exception e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, 0);
            }
            stream.flush();
            stream.close();
        }
    }
    public static final MediaType JSON= MediaType.parse("application/json; charset=utf-8");
    public static byte[] request( HttpExchange exchange ) throws IOException, ParseException
    {
        // System.out.println(url);
        String hostPort="",protcaluserpass="",protocol="",username="",password="",hostname="";
        int port;
        JSONObject body = App.getRequestBodyJson(exchange);
        String url = (String) body.get("url");
        JSONObject headers = (JSONObject) body.get("headers");
        Builder Tmprequest = new Request.Builder()
          .url(url);
        // System.out.println(body.get("method"));
        if (body.get("method")!=null && body.get("method").equals("post") ){
            JSONObject bd =  (JSONObject) body.get("body");
            RequestBody reqbody = RequestBody.create(JSON,bd.toJSONString()); // new
            Tmprequest = Tmprequest.post(reqbody);
        }else{
            Tmprequest = Tmprequest.get();
        }
        if (body.get("proxy") != null){
            String proxyurl = (String) body.get("proxy");
            if (proxyurl.split("@").length==2){
                hostPort = proxyurl.split("@")[1];
                protcaluserpass = proxyurl.split("@")[0];
                protocol =protcaluserpass.split("://")[0];
                username =protcaluserpass.split("://")[1].split(":")[0];
                password =protcaluserpass.split("://")[1].split(":")[1];
            }else{
                protocol = proxyurl.split("://")[0].substring(0, 4);
                hostPort = proxyurl.split("://")[1]; 
                username= "";
                password= "";   
            }
            hostname = hostPort.split(":")[0];
            port = Integer.parseInt(hostPort.split(":")[1]);
            
            Proxy proxy;
            if (protocol.equals("http")) {
                proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(hostname, port));
            }
            else {
                proxy = new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(hostname, port));
            }
            // Set the default authenticator to provide credentials for the proxy
            // /user-sp30786500:Engineer12@
            
            client = client.setProxy(proxy);
            if (username!=""){
                client = client.setAuthenticator(new MyProxyAuthenticator(username,password));
            }
        }
        // else{
        //     client = new OkHttpClient();
        // }
        for(Iterator iterator = headers.keySet().iterator(); iterator.hasNext();) {
            String key = (String) iterator.next();
            // System.out.println(headers.get(key));
            try {
                Tmprequest.addHeader(key, (String) headers.get(key));
            } catch (Exception e) {
                // TODO: handle exception
            }
            // System.out.println(key+ (String) headers.get(key));
        }
        //   .addHeader("accept", "application/json")
        //   .addHeader("cache-control", "no-cache")
        //   .addHeader("uniquedeviceid", "51268a31a20336c9")
        //   .addHeader("appversion", "1.71.2.3200232")
        //   .addHeader("apporigin", "android")
        //   .addHeader("authorization", "undefined")
        //   .addHeader("accept-language", "en-US")
        //   .addHeader("apptoken", "524b9c59052b5c91f5e78e282ac46b71")
        //   .addHeader("appvanity", "6362b08118810")
        //   .addHeader("Host", "okhttpserver.com")
        //   .addHeader("Connection", "Keep-Alive")
        //   .addHeader("Accept-Encoding", "gzip")
        //   .addHeader("User-Agent", "okhttp/4.9.2")
        Request request= Tmprequest.build();
        System.out.println(username + password+ hostname);
        Response response = client.newCall(request).execute();
        // for (Map.Entry<String, List<String>> entry : response.headers().toMultimap().entrySet()) {
        //     System.out.println(entry.getValue());
        //     if(entry.getKey()=="Set-Cookie"){
        //         exchange.getResponseHeaders().set(entry.getKey() ,String.join(";",entry.getValue()));
        //         System.out.println(entry.getKey() +String.join(";",entry.getValue()));
        //     }
        // }
        try {
            exchange.getResponseHeaders().set( "Set-Cookie",String.join(";",response.headers().toMultimap().get("Set-Cookie")));
        } catch (Exception e) {
            // TODO: handle exception
        }
        // if (response.code()!=200){
        //     client = new OkHttpClient();
        // }
        String contentEncoding = response.headers().get("Content-Encoding");
        
        byte[] res;
        if (contentEncoding!=null && contentEncoding.equals("gzip")){
            GzipSource responseBody = new GzipSource(response.body().source());
            Buffer result = new Buffer();
            while (responseBody.read(result, Integer.MAX_VALUE) != -1) ;
            res = result.readByteArray();
        }
        else{
            res =  response.body().string().getBytes("UTF-8");
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(response.code(), res.length);
        return res;
        // String jsonData = response.body().string();
        // JSONParser parser = new JSONParser();  
        // JSONObject json = (JSONObject) parser.parse(jsonData);  
    }
    


 }
