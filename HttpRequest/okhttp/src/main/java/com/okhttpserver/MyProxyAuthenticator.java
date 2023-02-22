package com.okhttpserver;
import java.io.IOException;
import java.net.Proxy;

import com.squareup.okhttp.Authenticator;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.Credentials;
public class MyProxyAuthenticator implements Authenticator {
    
    private String username;
    private String password;
    
    public MyProxyAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }
    

    @Override
    public Request authenticate(Proxy arg0, Response arg1) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Request authenticateProxy(Proxy arg0, Response response) throws IOException {
        String credential = Credentials.basic(username, password);
        return response.request().newBuilder()
                .header("Proxy-Authorization", credential)
                .build();
    }

}


