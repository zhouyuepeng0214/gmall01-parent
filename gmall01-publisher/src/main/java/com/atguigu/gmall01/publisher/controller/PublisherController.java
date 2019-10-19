package com.atguigu.gmall01.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall01.publisher.bean.Option;
import com.atguigu.gmall01.publisher.bean.Stat;
import com.atguigu.gmall01.publisher.service.PublisherService;
import jdk.nashorn.internal.runtime.linker.LinkerCallSite;
import jline.internal.Log;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

	@Autowired
	PublisherService publisherService;

	@GetMapping("realtime-total")
	public String getTotal(@RequestParam("date") String date) {
		Long dauTotal = publisherService.getDauTotal(date);

		Map dauMap = new HashMap();
		dauMap.put("id", "dau");
		dauMap.put("name", "新增日活");
		dauMap.put("value", dauTotal);

		List<Map> totalList = new ArrayList<>();
		totalList.add(dauMap);

		Map newMidMap = new HashMap();
		newMidMap.put("id", "new_mid");
		newMidMap.put("name", "新增设备");
		newMidMap.put("value", 1000);
		totalList.add(newMidMap);

		Map orderAmountMap = new HashMap();
		orderAmountMap.put("id", "order_amount");
		orderAmountMap.put("name", "新增交易额");
		Double orderAmount = publisherService.getOrderAmount(date);
		orderAmountMap.put("value",orderAmount);
		totalList.add(orderAmountMap);

		return JSON.toJSONString(totalList);
	}

	@GetMapping("realtime-hour")
	public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date){
		if("dau".equals(id)){
			Map<String, Long> dauHourCountTodayMap = publisherService.getDauHourCount(date);
			String yesterday = getYesterday(date);
			Map<String, Long> dauHourYDayCountMap = publisherService.getDauHourCount(yesterday);
			Map dauMap = new HashMap();
			dauMap.put("today", dauHourCountTodayMap);
			dauMap.put("yesterday", dauHourYDayCountMap);

			return JSON.toJSONString(dauMap);
		}else if("order_amount".equals(id)){
			//新增设备业务
			Map<String, Double> orderHourAmountTodayMap = publisherService.getOrderHourAmount(date);
			String yesterday = getYesterday(date);
			Map<String, Double> orderHourAmountYdayMap = publisherService.getOrderHourAmount(yesterday);
			Map orderAmountMap = new HashMap();

			orderAmountMap.put("today",orderHourAmountTodayMap);
			orderAmountMap.put("yesterday",orderHourAmountYdayMap);

			return JSON.toJSONString(orderAmountMap);

		}
		return null;
	}

	@GetMapping("sale_detail")
	public String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword") String keyword,@RequestParam("startpage") int startpage,@RequestParam("size") int size) {

		Map<String, Object> saledtailMap = publisherService.getSaleDetailFromES(date, keyword, startpage, size);
		Long total = (Long) saledtailMap.get("total");
		List saleList = (List) saledtailMap.get("saleList");
		Map genderMap = (Map) saledtailMap.get("genderMap");
		Map ageMap = (Map) saledtailMap.get("ageMap");

		Long maleCount = (Long) genderMap.get("M");
		Long femaleCount = (Long) genderMap.get("F");

		double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
		double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;

		List genderOptionList = new ArrayList();
		genderOptionList.add(new Option("男",maleRatio));
		genderOptionList.add(new Option("女",femaleRatio));

		Stat genderStat = new Stat("性别占比", genderOptionList);

		Long age_20count = 0L;
		Long age20_30count = 0L;
		Long age30_count = 0L;

		for (Object o : ageMap.entrySet()) {
			Map.Entry entry = (Map.Entry) o;
			String ageString = (String) entry.getKey();
			Long ageCount = (Long) entry.getKey();
			if(Integer.parseInt(ageString) < 20) {
				age_20count += ageCount;
			} else if (Integer.parseInt(ageString) >= 20 && Integer.parseInt(ageString) <= 30) {
				age20_30count += ageCount;
			} else {
				age30_count += ageCount;
			}
		}

		Double age_20Ratio = Math.round(age_20count * 1000D / total) / 10D;
		Double age20_30Ratio = Math.round(age20_30count * 1000D / total) / 10D;
		Double age30_Ratio = Math.round(age30_count * 1000D / total) / 10D;

		List ageOptionList = new ArrayList();
		ageOptionList.add(new Option("20岁以下",age_20Ratio));
		ageOptionList.add(new Option("20岁到30岁",age20_30Ratio));
		ageOptionList.add(new Option("30岁以上",age30_Ratio));

		Stat ageStat = new Stat("年龄段占比", ageOptionList);

		List statList = new ArrayList();
		statList.add(genderStat);
		statList.add(ageStat);

		Map finalResultMap = new HashMap();
		finalResultMap.put("total",total);
		finalResultMap.put("stat",statList);
		finalResultMap.put("detail",saleList);

		return JSON.toJSONString(finalResultMap);


	}


	private String getYesterday(String todayStr){
		String yesterdayStr = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date today = dateFormat.parse(todayStr);
			Date yesterday = DateUtils.addDays(today, -1);
			yesterdayStr = dateFormat.format(yesterday);
		} catch (ParseException e) {
			Log.error(e, e.getMessage());
		}
		return yesterdayStr;
	}

}
