package com.isesol.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;  
import org.apache.hadoop.hbase.CellUtil;  
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.isesol.mapreduce.Sum.SumRequest;
import com.isesol.mapreduce.Sum.SumResponse;
import com.isesol.mapreduce.Sum.SumService;

public class endpointTrigger extends SumService implements Coprocessor, CoprocessorService {

	private RegionCoprocessorEnvironment env;

	@Override
	public void getSum(RpcController controller, SumRequest request, RpcCallback<SumResponse> done) {
		// TODO Auto-generated method stub

		SumResponse response = null;
		InternalScanner scanner = null;
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(request.getFamily()));
		//scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));
		// 扫描每个region，取值后求和
		try {
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMore = false;
			Long sum = 0L;
			do {
				hasMore = scanner.next(results);
				for (Cell cell : results) {
					//sum += Long.parseLong(new String(CellUtil.cloneValue(cell)));
					sum += 1;
				}
				results.clear();
			} while (hasMore);
			// 设置返回结果
			response = SumResponse.newBuilder().setSum(sum).build();
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		done.run(response);
	}

	public Service getService() {
		// TODO Auto-generated method stub
		return this;
	}

	public void start(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("can not start endpoint");
		}

	}

	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

	}

}
