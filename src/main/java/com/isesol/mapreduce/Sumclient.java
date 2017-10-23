package com.isesol.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import com.google.protobuf.ServiceException;
import com.isesol.mapreduce.Sum.SumRequest;
import com.isesol.mapreduce.Sum.SumResponse;
import com.isesol.mapreduce.Sum.SumService;;


/**
 * @author developer
 * 说明：hbase协处理器endpooint的客户端代码
 * 功能：从服务端获取对hbase表指定列的数据的求和结果
 */
public class Sumclient {

    public static void main(String[] args) throws ServiceException, Throwable {
        
        long sum = 0L;
        
        int count = 0;
        
        // 配置HBse
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 建立一个数据库的连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf("test5"));
        // 设置请求对象
       // final SumRequest request = SumRequest.newBuilder().setFamily("cf").setColumn("sys_time").build();
        final SumRequest request = SumRequest.newBuilder().setFamily("cf").build();
        System.out.println("start to invoke result");
        
        // 获得返回值
        Map<byte[], Long> result = table.coprocessorService(Sum.SumService.class, null, null, 
                new Batch.Call<Sum.SumService, Long>() {

                    public Long call(SumService service) throws IOException {
                        BlockingRpcCallback<SumResponse> rpcCallback = new BlockingRpcCallback<SumResponse>();
                        service.getSum(null, request, rpcCallback);
                        SumResponse response = (SumResponse) rpcCallback.get();
                        return response.hasSum() ? response.getSum() : 0L;
                    }
        });
        
        System.out.println("satrt to count the value");
        // 将返回值进行迭代相加
        for (Long v : result.values()) {
        	System.out.println(v);
            count+=v;
        }
        // 结果输出
        System.out.println("count: " + count);
        // 关闭资源
        table.close();
        conn.close();
    }

}