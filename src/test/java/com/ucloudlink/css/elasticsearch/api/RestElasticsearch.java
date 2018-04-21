package com.ucloudlink.css.elasticsearch.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.UUIDs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ucloudlink.css.elasticsearch.config.ElasticsearchSingleton;
import com.ucloudlink.css.util.HttpUtil;
import com.ucloudlink.css.util.StringUtil;

public class RestElasticsearch {
//	private static Logger logger = LogManager.getLogger();
	protected static String regex = "[-,:,/\"]";
	protected RestClient client = ElasticsearchSingleton.getIntance().client();
	public String indices() throws Exception {
		String uri = "/_cat/indices?v";
		HttpEntity entity = null;
		Response response = client.performRequest(HttpUtil.METHOD_GET, uri,Collections.singletonMap("pretty", "true"),entity);
		String result  = EntityUtils.toString(response.getEntity());
		if(!StringUtil.isEmpty(result)){
			String[] data = result.split("\n");
			if(data!=null&&data.length>1){
				List<JSONObject> list = new ArrayList<JSONObject>();
				String[] header = data[0].split("(\\s+)");
				for(int i=1;i<data.length;i++){
					String[] values = data[i].split("(\\s+)");
					JSONObject json = new JSONObject();
					for(int j=0;j<values.length;j++){
						json.put(header[j], values[j]);
					}
					list.add(json);
				}
				if(list!=null&&list.size()==1){
					return list.get(0).toJSONString();
				}else{
					return JSON.toJSONString(list);
				}
			}
		}else{
			result = response.getStatusLine().getReasonPhrase();
		}
		return result;
	}
	public String nodes() throws Exception {
		String uri = "/_cat/nodes?v";
		HttpEntity entity = null;
		Response response = client.performRequest(HttpUtil.METHOD_GET, uri,Collections.singletonMap("pretty", "true"),entity);
		String result  = EntityUtils.toString(response.getEntity());
		if(!StringUtil.isEmpty(result)){
			String[] data = result.split("\n");
			if(data!=null&&data.length>1){
				List<JSONObject> list = new ArrayList<JSONObject>();
				String[] header = data[0].split("(\\s+)");
				for(int i=1;i<data.length;i++){
					String[] values = data[i].split("(\\s+)");
					JSONObject json = new JSONObject();
					for(int j=0;j<values.length;j++){
						json.put(header[j], values[j]);
					}
					list.add(json);
				}
				if(list!=null&&list.size()==1){
					return list.get(0).toJSONString();
				}else{
					return JSON.toJSONString(list);
				}
			}
		}else{
			result = response.getStatusLine().getReasonPhrase();
		}
		return result;
	}
	public String insert(String index, String type, Object obj) throws Exception {
		String uri = "/"+index+"/"+type;
		HttpEntity entity = null;
		if(obj!=null){
			String body = JSON.toJSONString(obj);
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response =  client.performRequest(HttpUtil.METHOD_POST,uri,Collections.emptyMap(), entity);
		return EntityUtils.toString(response.getEntity());
	}

	public String update(String index, String type, String id, Object obj) throws Exception {
		String uri = "/"+index+"/"+type+"/"+id;
		HttpEntity entity = null;
		Response response =  client.performRequest(HttpUtil.METHOD_GET,uri,Collections.emptyMap(),entity);
		JSONObject target = JSON.parseObject(EntityUtils.toString(response.getEntity()));
		if(target.getBooleanValue("found")){
			if(obj!=null){
				String body = JSON.toJSONString(obj);
				entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
			}
			response =  client.performRequest(HttpUtil.METHOD_GET,uri,Collections.emptyMap(),entity);
		}
		return EntityUtils.toString(response.getEntity());
	}

	public String upsert(String index, String type, String id, Object obj) throws Exception {
		String uri = "/"+index+"/"+type+"/"+id;
		HttpEntity entity = null;
		if(obj!=null){
			String body = obj instanceof String ?(String)obj:JSON.toJSONString(obj);
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_PUT, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	public String delete(String index, String type, String id) throws Exception {
		String uri = "/"+index+"/"+type+"/"+id;
		HttpEntity entity = null;
		Response response = client.performRequest(HttpUtil.METHOD_DELETE, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	public String bulkUpsert(String index, String type, List<Object> jsons) throws Exception {
		String uri = "/_bulk";
		String body = "";
		for (Object json : jsons) {
			String source = json instanceof String ?json.toString():JSON.toJSONString(json);
			JSONObject obj = JSON.parseObject(source);
			String id = null;
			if(obj.containsKey("id")||obj.containsKey("_id")){
				if(obj.containsKey("_id")){
					id = obj.getString("_id");
					obj.remove("_id");
				}else{
					id = obj.getString("id");
					obj.remove("id");
				}
			}
			if(!StringUtil.isEmpty(id)){
				String action = "{update:{_index:'"+index+"',_type:'"+type+"',_id:'"+id+"'}}";
				String _body ="{doc:"+obj.toJSONString()+"}";
				body += JSON.parseObject(action).toJSONString()+"\n"+JSON.parseObject(_body).toJSONString()+"\n";
			}else{
				id = UUIDs.base64UUID();
				String action = "{create:{_index:'"+index+"',_type:'"+type+"',_id:'"+id+"'}}";
				body += JSON.parseObject(action).toJSONString()+"\n"+obj.toJSONString()+"\n";
			}
		}
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}

	public String bulkDelete(String index, String type, String... ids) throws Exception {
		String uri = "/_bulk";
		String body = "";
		for (String id : ids) {
			String action = "{delete:{_index:'"+index+"',_type:'"+type+"',_id:'"+id+"'}}";
			body += JSON.parseObject(action).toJSONString()+"\n";
		}
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	public String drop(String indexs) throws Exception {
		String uri = "/"+indexs;
		HttpEntity entity = null;
		Response response = client.performRequest(HttpUtil.METHOD_DELETE, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	public String select(String index, String type, String id) throws Exception {
		String uri = "/"+index+"/"+type+"/"+id;
		HttpEntity entity = null;
		Response response = client.performRequest(HttpUtil.METHOD_GET, uri,Collections.singletonMap("pretty", "true"),entity);
		return EntityUtils.toString(response.getEntity());
	}

	public String selectAll(String indexs, String types, String condition) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		if(!StringUtil.isEmpty(condition))uri+="&q="+HttpUtil.encode(condition);
		HttpEntity entity = null;
		Response response = client.performRequest(HttpUtil.METHOD_GET, uri,Collections.singletonMap("pretty", "true"),entity);
		return EntityUtils.toString(response.getEntity());
	}

	public String selectMatchAll(String indexs, String types, String field, String value) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		String body = "";
		if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
			String query = "{query:{match:{"+field+":'"+value+"'}}}";
			body = JSON.parseObject(query).toJSONString();
		}
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}

	public String selectMatchAll(String indexs, String types,Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		List<JSONObject> must_matchs = new ArrayList<JSONObject>();
		List<JSONObject> should_matchs = new ArrayList<JSONObject>();
		List<JSONObject> must_not_matchs = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must:"+JSON.toJSONString(child_matchs)+"}}";
						must_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{should:"+JSON.toJSONString(child_matchs)+"}}";
						must_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							should_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must_not:"+JSON.toJSONString(child_matchs)+"}}";
						must_not_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_not_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					start = start!=null&&start instanceof Date?((Date)start).getTime():start;
					end = end!=null&&end instanceof Date?((Date)end).getTime():end;
					if(start!=null&&end!=null){
						double starttime = Double.valueOf(start.toString());
						double endtime = Double.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					String range = "{range:{"+key+":{gte:"+start+",lt:"+end+"}}}";
					must_matchs.add(JSON.parseObject(range));
				}
			};
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_matchs)+",must_not:"+JSON.toJSONString(must_not_matchs)+",should:"+JSON.toJSONString(should_matchs)+"}}}";
		String body = JSON.parseObject(query).toJSONString();
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	
	public String selectMatchAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo,int pageSize) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty&size="+pageSize+"&from="+(pageNo-1)*pageSize;
		List<JSONObject> must_matchs = new ArrayList<JSONObject>();
		List<JSONObject> should_matchs = new ArrayList<JSONObject>();
		List<JSONObject> must_not_matchs = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must:"+JSON.toJSONString(child_matchs)+"}}";
						must_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{should:"+JSON.toJSONString(child_matchs)+"}}";
						must_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							should_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must_not:"+JSON.toJSONString(child_matchs)+"}}";
						must_not_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_not_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					start = start!=null&&start instanceof Date?((Date)start).getTime():start;
					end = end!=null&&end instanceof Date?((Date)end).getTime():end;
					if(start!=null&&end!=null){
						double starttime = Double.valueOf(start.toString());
						double endtime = Double.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					String range = "{range:{"+key+":{gte:"+start+",lt:"+end+"}}}";
					must_matchs.add(JSON.parseObject(range));
				}
			};
		}
		List<JSONObject> sorts = new ArrayList<JSONObject>();
		sorts.add(JSON.parseObject("{_score:{order:'desc'}}"));
		if(!StringUtil.isEmpty(order)){
			sorts.add(JSON.parseObject("{"+order+":{order:'"+(isAsc?"asc":"desc")+"'}}"));
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_matchs)+",must_not:"+JSON.toJSONString(must_not_matchs)+",should:"+JSON.toJSONString(should_matchs)+"}},sort:"+JSON.toJSONString(sorts)+"}";
		String body = JSON.parseObject(query).toJSONString();
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	public String selectPrefixMatchAll(String indexs, String types, Map<String, String> prefix, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, int pageNo,int pageSize) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty&size="+pageSize+"&from="+(pageNo-1)*pageSize;
		List<JSONObject> must_matchs = new ArrayList<JSONObject>();
		List<JSONObject> should_matchs = new ArrayList<JSONObject>();
		List<JSONObject> must_not_matchs = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must:"+JSON.toJSONString(child_matchs)+"}}";
						must_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{should:"+JSON.toJSONString(child_matchs)+"}}";
						must_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							should_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_matchs = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								child_matchs.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must_not:"+JSON.toJSONString(child_matchs)+"}}";
						must_not_matchs.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_not_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					start = start!=null&&start instanceof Date?((Date)start).getTime():start;
					end = end!=null&&end instanceof Date?((Date)end).getTime():end;
					if(start!=null&&end!=null){
						double starttime = Double.valueOf(start.toString());
						double endtime = Double.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					String range = "{range:{"+key+":{gte:"+start+",lt:"+end+"}}}";
					must_matchs.add(JSON.parseObject(range));
				}
			};
		}
		if(prefix!=null&&prefix.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				String value = prefix.get(field);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					String match = "{prefix:{"+field+":'"+value+"'}}";
					must_matchs.add(JSON.parseObject(match));
				}
			}
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_matchs)+",must_not:"+JSON.toJSONString(must_not_matchs)+",should:"+JSON.toJSONString(should_matchs)+"}}}";
		String body = JSON.parseObject(query).toJSONString();
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	public String selectTermAll(String indexs, String types, String field, String value) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		String body = "";
		if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
			String query = "{query:{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}}";
			body = JSON.parseObject(query).toJSONString();
		}
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}

	public String selectTermAll(String indexs, String types,Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		List<JSONObject> must_terms = new ArrayList<JSONObject>();
		List<JSONObject> should_terms = new ArrayList<JSONObject>();
		List<JSONObject> must_not_terms = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_terms.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{should:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							should_terms.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must_not:"+JSON.toJSONString(child_terms)+"}}";
						must_not_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_not_terms.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					start = start!=null&&start instanceof Date?((Date)start).getTime():start;
					end = end!=null&&end instanceof Date?((Date)end).getTime():end;
					if(start!=null&&end!=null){
						double starttime = Double.valueOf(start.toString());
						double endtime = Double.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					String range = "{range:{"+key+":{gte:"+start+",lt:"+end+"}}}";
					must_terms.add(JSON.parseObject(range));
				}
			};
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_terms)+",must_not:"+JSON.toJSONString(must_not_terms)+",should:"+JSON.toJSONString(should_terms)+"}}}";
		String body = JSON.parseObject(query).toJSONString();
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
	public String selectTermAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo, int pageSize) throws Exception {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty&size="+pageSize+"&from="+(pageNo-1)*pageSize;
		List<JSONObject> must_terms = new ArrayList<JSONObject>();
		List<JSONObject> should_terms = new ArrayList<JSONObject>();
		List<JSONObject> must_not_terms = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_terms.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{should:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							should_terms.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must_not:"+JSON.toJSONString(child_terms)+"}}";
						must_not_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_not_terms.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					start = start!=null&&start instanceof Date?((Date)start).getTime():start;
					end = end!=null&&end instanceof Date?((Date)end).getTime():end;
					if(start!=null&&end!=null){
						double starttime = Double.valueOf(start.toString());
						double endtime = Double.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					String range = "{range:{"+key+":{gte:"+start+",lt:"+end+"}}}";
					must_terms.add(JSON.parseObject(range));
				}
			};
		}
		List<JSONObject> sorts = new ArrayList<JSONObject>();
		sorts.add(JSON.parseObject("{_score:{order:'desc'}}"));
		if(!StringUtil.isEmpty(order)){
			sorts.add(JSON.parseObject("{"+order+":{order:'"+(isAsc?"asc":"desc")+"'}}"));
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_terms)+",must_not:"+JSON.toJSONString(must_not_terms)+",should:"+JSON.toJSONString(should_terms)+"}},sort:"+JSON.toJSONString(sorts)+"}";
		String body = JSON.parseObject(query).toJSONString();
		HttpEntity entity = null;
		if(body!=null){
			entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
		}
		Response response = client.performRequest(HttpUtil.METHOD_POST, uri,Collections.emptyMap(),entity);
		return EntityUtils.toString(response.getEntity());
	}
}