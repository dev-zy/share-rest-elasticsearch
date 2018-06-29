package com.devzy.share.elasticsearch.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import com.devzy.share.util.StringUtil;

public class ElasticsearchHighRestFactory{
	private static Logger logger = LogManager.getLogger();
	private static int DEFAULT_PORT = 9200;
	private static String DEFAULT_CLUSTERNAME = "elasticsearch";
	private static String DEFAULT_SERVERS = "localhost";
	
	private RestClient client = null;
	private RestHighLevelClient hclient=null;
	
	private String clusterName;
	private String servers;
	private String username;
	private String password;
	private int port;
	
	public ElasticsearchHighRestFactory() {
		this(DEFAULT_SERVERS,DEFAULT_PORT);
	}
	public ElasticsearchHighRestFactory(String servers) {
		this(servers,DEFAULT_PORT);
	}
	public ElasticsearchHighRestFactory(String servers,int port) {
		this(DEFAULT_CLUSTERNAME,servers,port);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers,int port) {
		this(clusterName,servers,null,null,port);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password) {
		this(clusterName,servers,null,null,DEFAULT_PORT);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password,int port) {
		this.clusterName = clusterName;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.port = port>0?port:DEFAULT_PORT;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public String getServers() {
		return servers;
	}
	public void setServers(String servers) {
		this.servers = servers;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	/**
	 * 描述: Elasticsearch服务初始化
	 * 时间: 2017年11月14日 上午10:55:02
	 * @author yi.zhang
	 */
	public void init(){
		try {
			List<HttpHost> list = new ArrayList<HttpHost>();
			for(String server : servers.split(",")){
				String[] address = server.split(":");
				String ip = address[0];
				int _port=port;
				if(address.length>1){
					_port = Integer.valueOf(address[1]);
				}
				list.add(new HttpHost(ip, _port, "http"));
			}
			HttpHost[] hosts = new HttpHost[list.size()];
			list.toArray(hosts);
			RestClientBuilder builder = RestClient.builder(hosts);
 			builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
 	            @Override
 	            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder config) {
 	               config.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(100).build());
 	               if(!StringUtil.isEmpty(username)&&!StringUtil.isEmpty(password)){
 	            	   final CredentialsProvider credential = new BasicCredentialsProvider();
 	            	   credential.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username, password));
 	            	   config.setDefaultCredentialsProvider(credential);
 	               }
 	               return config;
 	            }
 	        });
 			builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
 	            @Override
 	            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder config) {
 	            	config.setConnectTimeout(5*1000);
 	            	config.setSocketTimeout(60*1000);
 	                return config;
 	            }
 	        });
 			builder.setMaxRetryTimeoutMillis(60*1000);
 			client = builder.build();
 			hclient = new RestHighLevelClient(builder);
		} catch (Exception e) {
			logger.error("-----Elasticsearch Config init Error-----", e);
		}
	}
	public void close(){
		if(client!=null){
			try {
				client.close();
			} catch (IOException e) {
				logger.error("----Elasticsearch Rest Close Error!------------",e);
			}
		}
	}
	public RestClient client(){
		return client;
	}
	public RestHighLevelClient hclient(){
		return hclient;
	}
}