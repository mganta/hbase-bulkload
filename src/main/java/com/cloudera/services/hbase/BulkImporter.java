package com.cloudera.services.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;

public class BulkImporter extends Configured implements Tool {

	static String TABLE_NAME = "qualys";

	static class HBaseMapper extends
			Mapper<Object, BytesRefArrayWritable, ImmutableBytesWritable, Put> {
		static final byte[] COLUMN_FAMILY_CURRENT = Bytes.toBytes("curr");
		static final byte[] COLUMN_FAMILY_HISTORY = Bytes.toBytes("hist");

		@Override
		public void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {

			if (value.size() == 110) {
				byte[] rowKey = RowKeyConverter.makeRowKey(value.get(1)
						.getData(), value.get(2).getData(), value.get(3)
						.getData(), value.get(4).getData());
				Put p = new Put(rowKey);

				for (int i = 0; i < value.size(); i++) {
					BytesRefWritable v = value.get(i);
					p.addColumn(COLUMN_FAMILY_CURRENT, Bytes.toBytes("foo"),
							v.getData());
				}

				context.write(new ImmutableBytesWritable(rowKey), p);
			}
		}
	}

	public int run(String[] args) throws Exception {
		
		if (args.length != 1) {
			System.err.println("Usage: BulkImporter <input>");
			return -1;
		}
		
		Configuration conf = HBaseConfiguration.create(getConf());
		Job job = new Job(conf, getClass().getSimpleName());
		job.setJarByClass(getClass());
		Path input = new Path(args[0]);
		FileInputFormat.addInputPath(job, input);
		Path tmpPath = new Path("/tmp/bulk");
		FileOutputFormat.setOutputPath(job, tmpPath);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setMapperClass(HBaseMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Cell.class);
		job.setNumReduceTasks(0);

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		Admin admin = connection.getAdmin();
		RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(TABLE_NAME));

		try {
			HFileOutputFormat2.configureIncrementalLoad(job, table,
					regionLocator);
			if (!job.waitForCompletion(true)) {
				return 1;
			}

			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			loader.doBulkLoad(tmpPath, (HTable) table);

			FileSystem.get(conf).delete(tmpPath, true);
			return 0;
		} finally {
			table.close();
			admin.close();
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new BulkImporter(), args);
		System.exit(exitCode);
	}
}
