package com.atguigu.gmall01.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall01.common.constant.GmallConstants
import com.atguigu.gmall01.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall01.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

object AlertApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("alert_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)
    val eventInfoDstream: DStream[EventInfo] = inputDstream.map { record =>
      val eventInfo: EventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
      eventInfo
    }
    eventInfoDstream.cache()

    //    1 ) 5分钟内 --> 窗口大小  window      窗口 （窗口大小，滑动步长 ）   窗口大小 数据的统计范围    滑动步长 统计频率
    val eventWindowDStream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(5))

    val groupbyMidDStream: DStream[(String, Iterable[EventInfo])] = eventWindowDStream.map { eventIfo =>
      (eventIfo.mid, eventIfo)
    }.groupByKey()
    val checkedDStream: DStream[(Boolean, AlertInfo)] = groupbyMidDStream.map {
      case (mid, eventInfoItr) => {
        val couponUidSet = new util.HashSet[String]()
        val itemsSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        var hasClickItem = false
        Breaks.breakable {
          for (eventInfo: EventInfo <- eventInfoItr) {
            eventList.add(eventInfo.evid)
            if (eventInfo.evid == "coupon") {
              couponUidSet.add(eventInfo.uid)
              itemsSet.add(eventInfo.itemid)
            }
            if (eventInfo.evid == "clickItem") {
              hasClickItem = true
              Breaks.break()
            }
          }
        }

        (couponUidSet.size() >= 3 && !hasClickItem, AlertInfo(mid, couponUidSet, itemsSet, eventList, System.currentTimeMillis()))
      }
    }

//    checkedDStream.foreachRDD(rdd =>
//      println(rdd.collect().mkString("\n"))
//    )

    val alterDStream: DStream[AlertInfo] = checkedDStream.filter(_._1).map(_._2)

    // 保存到ES中

    alterDStream.foreachRDD{rdd =>
      rdd.foreachPartition{alterItr =>
        val list: List[AlertInfo] = alterItr.toList
        val alterListWithId: List[(String, AlertInfo)] = list.map(AlertInfo => (AlertInfo.mid + "_" + AlertInfo.ts/1000/60,AlertInfo))
        MyEsUtil.indexBUlk(GmallConstants.ES_INDEX_ALTER,alterListWithId)

      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
