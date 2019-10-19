package com.atguigu.gmall01.publisher.mapper;

import com.atguigu.gmall01.publisher.bean.OrderHourAmount;

import java.util.List;


public interface OrderMapper {

	public  Double  getOrderAmount(String date);

	public List<OrderHourAmount> getOrderHourAmount(String date);

}
