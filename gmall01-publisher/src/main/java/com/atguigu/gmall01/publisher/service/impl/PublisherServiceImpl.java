package com.atguigu.gmall01.publisher.service.impl;

import com.atguigu.gmall01.publisher.bean.OrderHourAmount;
import com.atguigu.gmall01.publisher.mapper.DauMapper;
import com.atguigu.gmall01.publisher.mapper.OrderMapper;
import com.atguigu.gmall01.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl  implements PublisherService {

	@Autowired
	DauMapper dauMapper;

	@Autowired
	OrderMapper orderMapper;

	@Autowired
	JestClient jestClient;

	@Override
	public Long getDauTotal(String date) {
		return dauMapper.getDauTotal(date);
	}

	@Override
	public Map<String, Long> getDauHourCount(String date) {

		List<Map> dauHourCountList = dauMapper.getDauHourCount(date);
		Map<String,Long> hourMap=new HashMap<>();

		for (Map map : dauHourCountList) {
			hourMap.put((String)map.get("LOGHOUR"),(Long)map.get("CT"));
		}

		return hourMap;
	}

	@Override
	public Double getOrderAmount(String date) {
		return orderMapper.getOrderAmount(date);
	}

	@Override
	public Map<String,Double> getOrderHourAmount(String date) {
		HashMap<String, Double> hourAmountMap = new HashMap<>();
		List<OrderHourAmount> orderHourAmountList = orderMapper.getOrderHourAmount(date);

		for (OrderHourAmount orderAmoutHour : orderHourAmountList) {
			hourAmountMap.put(orderAmoutHour.getCreateHour(),orderAmoutHour.getSumOrderAmount());
		}

		return hourAmountMap;
	}

	public Map<String,Object> getSaleDetailFromES(String date,String keyword,int pageNo,int pagesize) {

		//构造过滤，匹配条件
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
		boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
		searchSourceBuilder.query(boolQueryBuilder);

		//聚合
		TermsBuilder genderAggs = AggregationBuilders.terms("group_gender").field("user_gender").size(2);
		TermsBuilder ageAggs = AggregationBuilders.terms("group_age").field("user_age").size(100);
		searchSourceBuilder.aggregation(genderAggs);
		searchSourceBuilder.aggregation(ageAggs);

		//分页
		searchSourceBuilder.from((pageNo - 1) * pagesize);
		searchSourceBuilder.from(pagesize);
		System.out.println(searchSourceBuilder.toString());

		//处理返回结果

		Search search = new Search.Builder(searchSourceBuilder.toString()).build();
		Map<String,Object> resultMap = new HashMap<>();
		try {
			SearchResult searchResult = jestClient.execute(search);
			resultMap.put("total",searchResult.getTotal());
			List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
			List<Map> saleList = new ArrayList<>();
			for (SearchResult.Hit<Map,Void> hit : hits) {
				saleList.add(hit.source);
				
			}
			resultMap.put("saleList",saleList);

			Map genderMap = new HashMap();
			List<TermsAggregation.Entry> group_gender = searchResult.getAggregations().getTermsAggregation("group_gender").getBuckets();

			for (TermsAggregation.Entry entry : group_gender) {
				genderMap.put(entry.getKey(),entry.getCount());
			}
			resultMap.put("genderMap",genderMap);

			Map ageMap = new HashMap();
			List<TermsAggregation.Entry> group_age = searchResult.getAggregations().getTermsAggregation("group_age").getBuckets();

			for (TermsAggregation.Entry entry : group_age) {
				ageMap.put(entry.getKey(),entry.getCount());
			}
			resultMap.put("ageMap",ageMap);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultMap;
	}


}
