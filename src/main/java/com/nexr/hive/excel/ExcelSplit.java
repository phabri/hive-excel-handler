package com.nexr.hive.excel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

public class ExcelSplit extends FileSplit{

	public ExcelSplit(Path file, long start, long length, String[] hosts) {
		super(file, start, length, hosts);
	}

}
