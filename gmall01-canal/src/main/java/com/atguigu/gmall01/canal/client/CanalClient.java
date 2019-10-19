package com.atguigu.gmall01.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

	public static void main(String[] args) {

		CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop110",11111), "example", "", "");

		while (true) {
			canalConnector.connect();
			canalConnector.subscribe("gmall0311.*");
			Message message = canalConnector.get(100);

			if (message.getEntries().size() == 0) {
				System.out.println("暂时没有数据");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				for (CanalEntry.Entry entry : message.getEntries()) {
					//只有行变化才处理
					if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
						CanalEntry.RowChange rowChange = null;
						try {
							// 把entry中的数据进行反序列化
							rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
						} catch (InvalidProtocolBufferException e) {
							e.printStackTrace();
						}
						//行集
						List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
						CanalEntry.EventType eventType = rowChange.getEventType();//insert update delete
						String tableName = entry.getHeader().getTableName();
						CanalHanlder canalHanlder = new CanalHanlder(tableName, eventType, rowDatasList);

						canalHanlder.handle();
					}
				}
			}
		}
	}
}
