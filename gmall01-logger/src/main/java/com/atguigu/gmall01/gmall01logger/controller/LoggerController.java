package com.atguigu.gmall01.gmall01logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall01.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {

	@Autowired
	KafkaTemplate<String,String>kafkaTemplate;

	@PostMapping("log")
	public String doLog(@RequestParam("logString") String logString ){

		// 0 补充时间戳
		JSONObject jsonObject = JSON.parseObject(logString);
		jsonObject.put("ts",System.currentTimeMillis());
		// 1 落盘 file
		String jsonString = jsonObject.toJSONString();
		log.info(jsonObject.toJSONString());

		if ("startup".equals(jsonObject.getString("type"))) {
			kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
		} else {
			kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
		}

		return "success";

	}

}
