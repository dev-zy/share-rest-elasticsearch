package com.ucloudlink.css.elasticsearch;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.ucloudlink.css.elasticsearch.api.HighRestElasticsearch;
import com.ucloudlink.css.elasticsearch.api.RestElasticsearch;
import com.ucloudlink.css.elasticsearch.pojo.SampleEntity;
import com.ucloudlink.css.util.DateUtil;
import com.ucloudlink.css.util.StringUtil;

public class ElasticsearchRestTest {
	private static RestElasticsearch factory = new RestElasticsearch();
	private static HighRestElasticsearch hfactory = new HighRestElasticsearch();
	private static SampleEntity data(){
		SampleEntity obj = new SampleEntity();
		String let = String.valueOf((char)Integer.valueOf(StringUtil.random(2)).intValue());
		Random random =new Random();
		Date today = new Date();
		Date begintime = new Date(today.getTime()+random.nextInt());
		obj.setName("title "+let+" "+StringUtil.random(3));
		obj.setImei("138"+StringUtil.random(8));
		obj.setArray(new String[]{let,StringUtil.random(2)});
		obj.setBegintime(begintime);
		obj.setCreatetime(today);
		obj.setFlag(random.nextInt()%2==0);
		obj.setLen(random.nextLong());
		obj.setPrice(random.nextDouble());
		obj.setStatus(200);
		obj.setType(Integer.valueOf(StringUtil.random(1)));
		String content = "["+DateUtil.formatDateTimeStr(begintime)+"]"+"Elasticsearch 是一个分布式可扩展的实时搜索和分析引擎。它能帮助你搜索、分析和浏览数据，而往往大家并没有在某个项目一开始就预料到需要这些功能。Elasticsearch 之所以出现就是为了重新赋予硬盘中看似无用的原始数据新的活力。Elasticsearch 为很多世界流行语言提供良好的、简单的、开箱即用的语言分析器集合：阿拉伯语、亚美尼亚语、巴斯克语、巴西语、保加利亚语、加泰罗尼亚语、中文、捷克语、丹麦、荷兰语、英语、芬兰语、法语、加里西亚语、德语、希腊语、北印度语、匈牙利语、印度尼西亚、爱尔兰语、意大利语、日语、韩国语、库尔德语、挪威语、波斯语、葡萄牙语、罗马尼亚语、俄语、西班牙语、瑞典语、土耳其语和泰语";
		obj.setContent(content);
		return obj;
	}
	public static void write1() throws Exception{
		String index= "es_test";
		String type = "test";
		SampleEntity obj = data();
		String result = factory.insert(index, type, obj);
		System.out.println(result);
	}
	public static void write2() throws Exception{
		String index= "es_test";
		String type = "test";
		SampleEntity obj = data();
		String result = hfactory.insert(index, type, obj);
		System.out.println(result);
	}
	public static void read1() throws Exception{
		String index= "es_test";
		String type = "test";
		Map<String,Object> must = new HashMap<String, Object>();
		must.put("status", 200);
//		must.put("name", "title");
		Map<String,Object> should = new HashMap<String, Object>();
		Map<String,Object> must_not = new HashMap<String, Object>();
		must_not.put("flag", false);
		Map<String,List<Object>> ranges = new HashMap<String,List<Object>>();
		Date today = new Date();
		Date start = new Date(today.getTime()-30*24*60*60*1000l);
		Date end = new Date(today.getTime()+30*24*60*60*1000l);
		List<Object> range = Arrays.asList(new Object[]{start,end});
		ranges.put("begintime", range);
		String result = factory.selectMatchAll(index, type,must, should, must_not, ranges, "createtime", false, 1, 5);
		System.out.println(result);
	}
	public static void read21() throws Exception{
		String index= "es_test";
		String type = "test";
		Map<String,Object> must = new HashMap<String, Object>();
		must.put("status", 200);
//		must.put("name", "title");
		Map<String,Object> should = new HashMap<String, Object>();
		Map<String,Object> must_not = new HashMap<String, Object>();
		must_not.put("flag", false);
		Map<String,List<Object>> ranges = new HashMap<String,List<Object>>();
		Date today = new Date();
		Date start = new Date(today.getTime()-30*24*60*60*1000l);
		Date end = new Date(today.getTime()+30*24*60*60*1000l);
		List<Object> range = Arrays.asList(new Object[]{start,end});
		ranges.put("begintime", range);
		String result = hfactory.selectMatchAll(index, type,must, should, must_not, ranges, "createtime", false, 1, 5);
		System.out.println(result);
	}
	public static void read2() throws Exception{
		String index= "es_test";
		String type = "test";
		Map<String,String> prefix = new HashMap<String, String>();
		prefix.put("imei", "138");
		Map<String,Object> must = new HashMap<String, Object>();
		must.put("status", 200);
//		must.put("name", "title");
		Map<String,Object> should = new HashMap<String, Object>();
		Map<String,Object> must_not = new HashMap<String, Object>();
		must_not.put("flag", false);
		Map<String,List<Object>> ranges = new HashMap<String,List<Object>>();
		Date today = new Date();
		Date start = new Date(today.getTime()-30*24*60*60*1000l);
		Date end = new Date(today.getTime()+30*24*60*60*1000l);
		List<Object> range = Arrays.asList(new Object[]{start,end});
		ranges.put("begintime", range);
		String result =factory.selectPrefixMatchAll(index, type, prefix, must, should, must_not, ranges, 1, 5);
		System.out.println(result);
	}
	public static void read22() throws Exception{
		String index= "es_test";
		String type = "test";
		Map<String,String> prefix = new HashMap<String, String>();
		prefix.put("imei", "138");
		Map<String,Object> must = new HashMap<String, Object>();
		must.put("status", 200);
//		must.put("name", "title");
		Map<String,Object> should = new HashMap<String, Object>();
		Map<String,Object> must_not = new HashMap<String, Object>();
		must_not.put("flag", false);
		Map<String,List<Object>> ranges = new HashMap<String,List<Object>>();
		Date today = new Date();
		Date start = new Date(today.getTime()-30*24*60*60*1000l);
		Date end = new Date(today.getTime()+30*24*60*60*1000l);
		List<Object> range = Arrays.asList(new Object[]{start,end});
		ranges.put("begintime", range);
		String result =hfactory.selectPrefixMatchAll(index, type, prefix, must, should, must_not, ranges, 1, 5);
		System.out.println(result);
	}
	public static void main(String[] args) throws Exception {
//		write1();
		System.out.println("---------------------------------------------------------------------------------");
//		read1();
//		read2();
		System.out.println("---------------------------------------------------------------------------------");
		read21();
//		read22();
		System.out.println("---------------------------------------------------------------------------------");
		System.exit(1);
	}

}
