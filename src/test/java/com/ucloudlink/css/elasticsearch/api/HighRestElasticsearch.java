package com.ucloudlink.css.elasticsearch.api;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ucloudlink.css.elasticsearch.config.ElasticsearchSingleton;
import com.ucloudlink.css.util.StringUtil;

public class HighRestElasticsearch {
//	private static Logger logger = LogManager.getLogger();
	protected static String regex = "[-,:,/\"]";
	protected RestHighLevelClient hclient = ElasticsearchSingleton.getIntance().hclient();
	public String insert(String index,String type,Object json){
		try {
//			XContentBuilder builder = XContentFactory.jsonBuilder();
//			builder.startObject();
//			{
//			    builder.field("user", "kimchy");
//			    builder.field("postDate", new Date());
//			    builder.field("message", "trying out Elasticsearch");
//			}
//			builder.endObject();
			String source = json instanceof String ?json.toString():JSON.toJSONString(json);
			JSONObject body = JSON.parseObject(source);
			IndexRequest request = new IndexRequest(index, type);
			request.source(body,XContentType.JSON);
			IndexResponse response = hclient.index(request);
//			String _index = response.getIndex();
//			String _type = response.getType();
//			String id = response.getId();
//			long version = response.getVersion();
//			if (response.getResult() == DocWriteResponse.Result.CREATED) {
//			    
//			} else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
//			    
//			}
//			ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
//			if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
//			    
//			}
//			if (shardInfo.getFailed() > 0) {
//			    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
//			        String reason = failure.reason(); 
//			    }
//			}
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String update(String index,String type,String id,Object json){
		try {
			String source = json instanceof String ?json.toString():JSON.toJSONString(json);
			JSONObject body = JSON.parseObject(source);
			UpdateRequest request = new UpdateRequest(index, type, id);
			request.doc(body,XContentType.JSON);
			UpdateResponse response = hclient.update(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String upsert(String index,String type,String id,Object json){
		try {
			if(hclient==null){
				return null;
			}
			String source = json instanceof String ?json.toString():JSON.toJSONString(json);
			JSONObject body = JSON.parseObject(source);
//			IndexRequest indexRequest = new IndexRequest(index, type, id).source(json,XContentType.JSON);
//			UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(json,XContentType.JSON).upsert(indexRequest);
			UpdateRequest request = new UpdateRequest(index, type, id);
			request.upsert(body,XContentType.JSON);
			UpdateResponse response = hclient.update(request);
			return response.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String delete(String index,String type,String id){
		try {
			if(hclient==null){
				return null;
			}
			DeleteRequest request = new DeleteRequest(index, type, id);
			DeleteResponse result = hclient.delete(request);
			return result.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String bulkUpsert(String index,String type,List<Object> jsons){
		try {
			if(hclient==null){
				return null;
			}
			BulkRequest request = new BulkRequest();
			for (Object json : jsons) {
				String source = json instanceof String ?json.toString():JSON.toJSONString(json);
				JSONObject obj = JSON.parseObject(source);
				String id = UUIDs.base64UUID();
				if(obj.containsKey("id")){
					id = obj.getString("id");
					obj.remove("id");
				}
//				if(obj.containsKey("id")){
//					request.add(new UpdateRequest(index, type, id).doc(obj.toJSONString(),XContentType.JSON));
//				}else{
//					request.add(new IndexRequest(index, type).source(obj.toJSONString(),XContentType.JSON));
//				}
				request.add(new UpdateRequest(index, type, id).upsert(obj.toJSONString(),XContentType.JSON));
			}
			BulkResponse result = hclient.bulk(request);
			return result.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String bulkDelete(String index,String type,String... ids){
		try {
			if(hclient==null){
				return null;
			}
			BulkRequest request = new BulkRequest();
			for (String id : ids) {
				request.add(new DeleteRequest(index, type, id));
			}
			BulkResponse result = hclient.bulk(request);
			return result.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String select(String index,String type,String id){
		try {
			if(hclient==null){
				return null;
			}
			GetRequest request = new GetRequest(index, type, id);
			GetResponse result = hclient.get(request);
			return result.getSourceAsString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectAll(String indexs,String types,String condition){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(hclient==null){
				return null;
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(QueryBuilders.queryStringQuery(condition)); 
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public String selectMatchAll(String indexs,String types,String field,String value){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(hclient==null){
				return null;
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
				search.query(QueryBuilders.matchQuery(field, value));
			}
			search.aggregation(AggregationBuilders.terms("data").field(field+".keyword"));
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectMatchAll(String indexs,String types,Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(hclient==null){
				return null;
			}
			BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
			HighlightBuilder highlight = new HighlightBuilder();
			if(must!=null&&must.size()>0){
				for (String field : must.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.matchQuery(field, value));
							}
						}
					}
					if(text instanceof String)highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.matchQuery(field, value));
							}
						}
					}
					if(text instanceof String)highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.matchQuery(field, value));
							}
						}
					}
					if(text instanceof String)highlight.field(field);
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
						RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
						if(start!=null){
							range.gte(start);
						}
						if(end!=null){
							range.lt(end);
						}
						boolquery.must(range);
					}
				};
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(boolquery);
			search.highlighter(highlight);
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectMatchAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo,int pageSize) {
		try {
			pageNo=pageNo<1?1:pageNo;
			pageSize=pageSize<1?10:pageSize;
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(hclient==null){
				return null;
			}
			BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
			HighlightBuilder highlight = new HighlightBuilder();
			if(must!=null&&must.size()>0){
				for (String field : must.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.matchQuery(field, value));
							}
						}
					}
					if(text instanceof String)highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.matchQuery(field, value));
							}
						}
					}
					if(text instanceof String)highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.matchQuery(field, value));
							}
						}
					}
					if(text instanceof String)highlight.field(field);
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
						RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
						if(start!=null){
							range.gte(start);
						}
						if(end!=null){
							range.lt(end);
						}
						boolquery.must(range);
					}
				};
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(boolquery);
			search.highlighter(highlight);
			search.from((pageNo-1)*pageSize);
			search.size(pageSize);
			search.sort(SortBuilders.scoreSort());
			if(!StringUtil.isEmpty(order)){
				search.sort(SortBuilders.fieldSort(order).order(isAsc?SortOrder.ASC:SortOrder.DESC));
			}
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectPrefixMatchAll(String indexs, String types, Map<String, String> prefix, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, int pageNo,int pageSize) {
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		try{
			BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
			if(must!=null&&must.size()>0){
				for (String field : must.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.matchQuery(field, value));
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.matchQuery(field, value));
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<Object> values = JSON.parseArray(value, Object.class);
							for (Object _value : values) {
								if(_value instanceof String && ((String)_value).matches(regex)){
									continue;
								}
								child.should(QueryBuilders.matchQuery(field, value));
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.matchQuery(field, value));
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
						RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
						if(start!=null){
							range.gte(start);
						}
						if(end!=null){
							range.lt(end);
						}
						boolquery.must(range);
					}
				};
			}
			if(prefix!=null&&prefix.size()>0){
				for (String field : prefix.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					String value = prefix.get(field);
					boolquery.must(QueryBuilders.prefixQuery(field, value));
				}
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(boolquery);
			search.from((pageNo-1)*pageSize);
			search.size(pageSize);
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectTermAll(String indexs,String types,String field,String value){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(hclient==null){
				return null;
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
				search.query(QueryBuilders.termQuery(field, value));
			}
			search.aggregation(AggregationBuilders.terms("data").field(field+".keyword"));
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectTermAll(String indexs,String types,Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(hclient==null){
				return null;
			}
			BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
			HighlightBuilder highlight = new HighlightBuilder();
			if(must!=null&&must.size()>0){
				for (String field : must.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
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
						RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
						if(start!=null){
							range.gte(start);
						}
						if(end!=null){
							range.lt(end);
						}
						boolquery.must(range);
					}
				};
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(boolquery);
			search.highlighter(highlight);
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectTermAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo,int pageSize) {
		try {
			pageNo=pageNo<1?1:pageNo;
			pageSize=pageSize<1?10:pageSize;
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(hclient==null){
				return null;
			}
			BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
			HighlightBuilder highlight = new HighlightBuilder();
			if(must!=null&&must.size()>0){
				for (String field : must.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
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
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
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
						RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
						if(start!=null){
							range.gte(start);
						}
						if(end!=null){
							range.lt(end);
						}
						boolquery.must(range);
					}
				};
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(boolquery);
			search.highlighter(highlight);
			search.from((pageNo-1)*pageSize);
			search.size(pageSize);
			search.sort(SortBuilders.scoreSort());
			if(!StringUtil.isEmpty(order)){
				search.sort(SortBuilders.fieldSort(order).order(isAsc?SortOrder.ASC:SortOrder.DESC));
			}
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = hclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}