package com.ucloudlink.css.elasticsearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticsearchHighRestFactory extends ElasticsearchRestFactory{
	private static Logger logger = LogManager.getLogger();
	protected RestHighLevelClient xclient=null;
	
	public ElasticsearchHighRestFactory() {
		super();
	}
	public ElasticsearchHighRestFactory(String servers) {
		super(servers);
	}
	public ElasticsearchHighRestFactory(String servers,int port) {
		super(servers, port);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers,int port) {
		super(clusterName, servers, port);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password) {
		super(clusterName, servers, username, password);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password,int port) {
		super(clusterName, servers, username, password, port);
	}

	/**
	 * 描述: Elasticsearch服务初始化
	 * 时间: 2017年11月14日 上午10:55:02
	 * @author yi.zhang
	 */
	public void init(){
		try {
			super.init();
			xclient = new RestHighLevelClient(super.getClient());
		} catch (Exception e) {
			logger.error("-----Elasticsearch Config init Error-----", e);
		}
	}
	
	public RestHighLevelClient getXClient(){
		return xclient;
	}
}