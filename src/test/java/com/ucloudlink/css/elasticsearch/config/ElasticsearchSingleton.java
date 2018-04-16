package com.ucloudlink.css.elasticsearch.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import com.ucloudlink.css.elasticsearch.rest.ElasticsearchHighRestFactory;
import com.ucloudlink.css.elasticsearch.rest.ElasticsearchRestFactory;
import com.ucloudlink.css.util.StringUtil;
/**
 * 描述: Elasticsearch初始化实例
 * 时间: 2018年1月9日 上午11:18:20
 * @author yi.zhang
 * @since 1.0
 * JDK版本:1.8
 */
public class ElasticsearchSingleton {
	private static Logger logger = LogManager.getLogger();
	private static class InitSingleton{
		private final static ElasticsearchSingleton INSTANCE = new ElasticsearchSingleton();
	}
	private ElasticsearchRestFactory rest;
	private ElasticsearchHighRestFactory high;
	private ElasticsearchSingleton(){
		try {
			String clusterName = ElasticConfig.getProperty("elasticsearch.cluster.name");
			String servers = ElasticConfig.getProperty("elasticsearch.cluster.servers");
			String username = ElasticConfig.getProperty("elasticsearch.cluster.username");
			String password = ElasticConfig.getProperty("elasticsearch.cluster.password");
			String http_port = ElasticConfig.getProperty("elasticsearch.http.port");
			rest=rest(clusterName, servers, username, password, http_port);
			high=high(clusterName, servers, username, password, http_port);
		} catch (Exception e) {
			logger.error("--Elasticsearch init Error!",e);
		}
	}
	public final static ElasticsearchSingleton getIntance(){
		return InitSingleton.INSTANCE;
	}
	public RestClient client(){
		return rest.getClient();
	}
	public RestHighLevelClient hclient(){
		return high.getXClient();
	}
	/**
	 * 描述: Elasticsearch配置[Rest接口]
	 * 时间: 2018年1月9日 上午11:02:08
	 * @author yi.zhang
	 * @param clusterName	集群名
	 * @param servers		服务地址(多地址以','分割)
	 * @param username		认证用户名
	 * @param password		认证密码
	 * @param port			服务端口
	 * @return
	 */
	private ElasticsearchRestFactory rest(String clusterName,String servers,String username,String password,String port){
		try {
			ElasticsearchRestFactory factory = new ElasticsearchRestFactory(clusterName, servers, username, password);
			if(!StringUtil.isEmpty(port))factory = new ElasticsearchRestFactory(clusterName, servers, username, password,Integer.valueOf(port));
			factory.init();
			return factory;
		} catch (Exception e) {
			logger.error("--Rest Elasticsearch init Error!",e);
		}
		return null;
	}
	/**
	 * 描述: Elasticsearch配置[HighRest接口]
	 * 时间: 2018年1月9日 上午11:02:08
	 * @author yi.zhang
	 * @param clusterName	集群名
	 * @param servers		服务地址(多地址以','分割)
	 * @param username		认证用户名
	 * @param password		认证密码
	 * @param port			服务端口
	 * @return
	 */
	private ElasticsearchHighRestFactory high(String clusterName,String servers,String username,String password,String port){
		try {
			ElasticsearchHighRestFactory factory = new ElasticsearchHighRestFactory(clusterName, servers, username, password);
			if(!StringUtil.isEmpty(port))factory = new ElasticsearchHighRestFactory(clusterName, servers, username, password,Integer.valueOf(port));
			factory.init();
			return factory;
		} catch (Exception e) {
			logger.error("--High Rest Elasticsearch init Error!",e);
		}
		return null;
	}
}