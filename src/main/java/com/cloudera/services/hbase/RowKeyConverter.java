package com.cloudera.services.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyConverter {

	 private static final int KEY_WIDTH = 4 * Md5Utils.MD5_LENGTH + 4;
	 private static final int REGION_COUNT = 40;

	/**
	 * @return A row key whose format is:
	 */
	public static byte[] makeRowKey(byte[] f1, byte[] f2, byte[] f3, byte[] f4) {
	    byte[] f1hash = Md5Utils.md5sum(new String(f1));
	    byte[] f2hash = Md5Utils.md5sum(new String(f2));
	    byte[] f3hash = Md5Utils.md5sum(new String(f3));
	    byte[] f4hash = Md5Utils.md5sum(new String(f4));
	    byte[] rowkey = new byte[KEY_WIDTH];
	    byte [] salt = Bytes.toBytes(new Integer(new Long(System.currentTimeMillis()).hashCode()).shortValue() % REGION_COUNT);
	    		
	    int offset = 0;
	    offset = Bytes.putBytes(rowkey, offset, salt, 0, salt.length); 
	    offset = Bytes.putBytes(rowkey, offset, f1hash, 0, f1hash.length);
	    offset = Bytes.putBytes(rowkey, offset, f2hash, 0, f2hash.length);
	    offset = Bytes.putBytes(rowkey, offset, f3hash, 0, f3hash.length);
	    offset = Bytes.putBytes(rowkey, offset, f4hash, 0, f4hash.length);
		return rowkey;
	}
}