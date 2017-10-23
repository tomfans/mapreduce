package com.isesol.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;


public class ObserverTrigger extends BaseRegionObserver {

	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit,
			final Durability durability) throws IOException {

		Configuration conf = new Configuration();
		HTable table = new HTable(conf, "test2");
		Put put01 = new Put(put.getRow());
		put01.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("xxx"));
		table.put(put);
		table.close();

	}

}