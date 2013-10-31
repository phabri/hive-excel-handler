package com.nexr.hive.excel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

// meaningless. 그냥 파일스플릿 써도 됨.
public class ExcelSplit extends FileSplit {

	public ExcelSplit() {
		super((Path) null, 0, 0, new String[] {});
	}

	public ExcelSplit(Path file, long start, long length, String[] hosts) {
		super(file, start, length, hosts);
	}

}
