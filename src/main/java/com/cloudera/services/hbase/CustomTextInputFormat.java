package com.cloudera.services.hbase;

	import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.google.common.base.Charsets;

	/** An {@link InputFormat} for plain text files.  Files are broken into lines.
	 * Either linefeed or carriage-return are used to signal end of line.  Keys are
	 * the position in the file, and values are the line of text.. */
	@InterfaceAudience.Public
	@InterfaceStability.Stable
	public class CustomTextInputFormat extends FileInputFormat<LongWritable, Text> {

	  @Override
	  public RecordReader<LongWritable, Text> 
	    createRecordReader(InputSplit split,
	                       TaskAttemptContext context) {
	    String delimiter = context.getConfiguration().get(
	        "textinputformat.record.delimiter");
	    char[] chars = {'\u0001'};
	    byte[] recordDelimiterBytes = new String(chars).getBytes();
	    if (null != delimiter)
	      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
	    return new LineRecordReader(recordDelimiterBytes);
	  }

	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    final CompressionCodec codec =
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    if (null == codec) {
	      return true;
	    }
	    return codec instanceof SplittableCompressionCodec;
	  }

	}
