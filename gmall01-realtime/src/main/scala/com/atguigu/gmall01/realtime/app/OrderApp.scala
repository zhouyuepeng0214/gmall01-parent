package com.atguigu.gmall01.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall01.common.constant.GmallConstants
import com.atguigu.gmall01.realtime.bean.OrderInfo
import com.atguigu.gmall01.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))
    
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    val orderDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonstr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonstr, classOf[OrderInfo])
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)

      val hourStr: String = datetimeArr(1).split(":")(0)
      orderInfo.create_hour = hourStr

      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)

      orderInfo.consignee_tel = tuple._1 + "*******"

      orderInfo

    }

    orderDstream.foreachRDD {rdd =>
      rdd.saveToPhoenix("GMALL0311_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration, Some("hadoop110,hadoop111,hadoop112:2181"))
    }

    ssc.start()
    ssc.awaitTermination()
    
  }

}
