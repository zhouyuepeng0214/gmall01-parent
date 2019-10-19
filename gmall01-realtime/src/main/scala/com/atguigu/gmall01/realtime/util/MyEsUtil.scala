package com.atguigu.gmall01.realtime.util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConversions._

object MyEsUtil {

  private val ES_HOST = "http://hadoop110"
  private val ES_HTTP_PORT = 9200
  private var factory : JestClientFactory = null

  /**
    *
    *建立连接
    */
  private def build() : Unit= {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
    .maxTotalConnection(20)
    .connTimeout(10000).readTimeout(10000).build)
  }

  /**
    *
    *获取客户端
    */

  def getClient : JestClient = {
    if (factory == null) {
      build()
    }
    factory.getObject
  }

  /**
    *
    * 关闭客户端
    *
    */

  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def indexBUlk(indexName : String,dataList : List[(String,Any)]): Unit ={
    if (dataList.size > 0) {
      val jestClient: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for ((id,data) <- dataList) {
        val index = new Index.Builder(data).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()

      val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getFailedItems

      println("保存" + items.mkString(",") + "条")
      close(jestClient)

    }
  }


  def main(args: Array[String]): Unit = {
    val jestClient: JestClient = getClient
    val index = new Index.Builder(Customer0311_2("lisi",1000.0)).index("customer0311_2").`type`("doc").id("1").build()
    val message: String = jestClient.execute(index).getErrorMessage

    println(s"meessage = ${message}")
    close(jestClient)

  }

  case class Customer0311_2(customer_name : String,customer_amount : Double)

}
