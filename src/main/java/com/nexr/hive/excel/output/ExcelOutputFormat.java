package com.nexr.hive.excel.output;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class ExcelOutputFormat implements HiveOutputFormat<NullWritable, Text> {

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
			throws IOException {
	}

	@Override
	public RecordWriter<NullWritable, Text> getRecordWriter(
			FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
			throws IOException {
		throw new RuntimeException("Error: Hive should not invoke this method.");
	}

	@Override
	public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
			JobConf conf, Path path,
			Class<? extends Writable> aClass, boolean b, Properties properties,
			Progressable progressable)
			throws IOException {
		//		FileSystem fileSystem = path.getFileSystem(conf);
		//		FSDataOutputStream os = fileSystem.create(path, true);

		return new ExcelRecordWriter(conf, path);
	}

}
