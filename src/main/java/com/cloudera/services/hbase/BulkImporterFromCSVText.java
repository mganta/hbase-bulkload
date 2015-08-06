package com.cloudera.services.hbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BulkImporterFromCSVText extends Configured implements Tool {

	static final String TABLE_NAME = "qualys";
	static final int COLUMN_COUNT = 9;

	static class HBaseMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		static final byte[] COLUMN_FAMILY_CURRENT = Bytes.toBytes("c");
		static final byte[] COLUMN_FAMILY_HISTORY = Bytes.toBytes("h");

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split(",");

			if (words.length == COLUMN_COUNT) {
				
				byte[] rowKey = RowKeyConverter.makeRowKey(words[0].getBytes(), words[1].getBytes(),
						words[2].getBytes(), words[3].getBytes());

				Put p = new Put(rowKey);

				for (int i = 0; i < words.length; i++) {
					
					//TO-DO add a column name
					if(words[i] != null)
					p.addColumn(COLUMN_FAMILY_CURRENT, Bytes.toBytes(i), words[i].getBytes());
				}

				context.write(new ImmutableBytesWritable(rowKey), p);
			} else {
				System.out.println("skipping record " + value.toString());
			}
		}
	}

	public int run(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.println("Usage: BulkImporter <hbase-site.xml> <input> <output>");
			return -1;
		}
		
		//Hbase config
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(args[0]));
		Job job = new Job(conf, getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		//input & output paths
		Path input = new Path(args[1]);
		FileInputFormat.addInputPath(job, input);
		Path tmpPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, tmpPath);
		
		//input format settings
		job.setInputFormatClass(TextInputFormat.class);
		
		//Map settings
		job.setMapperClass(HBaseMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setOutputFormatClass(HFileOutputFormat2.class);

		//HFile settings
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		Admin admin = connection.getAdmin();
		RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(TABLE_NAME));
		try {
			  HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
			  HFileOutputFormat2.setOutputPath(job, tmpPath);
			  HFileOutputFormat2.setCompressOutput(job, true);
			  HFileOutputFormat2.setOutputCompressorClass(job, SnappyCodec.class);
			
			  //kick off MR job
			if (!job.waitForCompletion(true)) {
				return 1;
			}
			
			//change permissions so that HBase user can read it
			FileSystem fs =  FileSystem.get(conf);
			FsPermission changedPermission=new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
			fs.setPermission(tmpPath, changedPermission);
			List<String> files = getAllFilePath(tmpPath, fs);
			for (String file : files) {
				fs.setPermission(new Path(file), changedPermission);
				System.out.println("Changing permission for file " + file);
			}

			//bulk load hbase files
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			loader.doBulkLoad(tmpPath, (HTable) table);

			//delete the hfiles
			FileSystem.get(conf).delete(tmpPath, true);	
			return 0;
			
		} finally {
			table.close();
			admin.close();
		}
	}
	
	/***
	 * Given a path, list all folders and files
	 * @param filePath
	 * @param fs
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
	    List<String> fileList = new ArrayList<String>();
	    FileStatus[] fileStatus = fs.listStatus(filePath);
	    for (FileStatus fileStat : fileStatus) {
	        if (fileStat.isDirectory()) {
	        	fileList.add(fileStat.getPath().toString());
	            fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
	        } else {
	            fileList.add(fileStat.getPath().toString());
	        }
	    }
	    return fileList;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new BulkImporterFromCSVText(), args);
		System.exit(exitCode);
	}
}
