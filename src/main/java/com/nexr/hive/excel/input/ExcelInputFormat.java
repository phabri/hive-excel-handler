package com.nexr.hive.excel.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.hive.excel.ExcelRowWritable;
import com.nexr.hive.excel.ExcelSplit;

public class ExcelInputFormat extends HiveInputFormat<LongWritable, ExcelRowWritable> {
	private static final Logger logger = LoggerFactory.getLogger(ExcelInputFormat.class);

	@Override
	public RecordReader<LongWritable, ExcelRowWritable> getRecordReader(
			InputSplit split, JobConf conf,
			Reporter reporter) throws IOException {
		logger.info("_______________________________________getRecordReader");
		logger.info(String.format("hive.excel.file.path=%s", conf.get("hive.excel.file.path")));
		logger.info(String.format("hive.excel.sheet.name=%s", conf.get("hive.excel.sheet.name")));
		logger.info(String.format("hive.excel.sheet.index=%s", conf.get("hive.excel.sheet.index")));

		if (!(split instanceof ExcelSplit)) {
			throw new IOException("invalid input split");
		}

		return new ExcelRecordReader(conf, (ExcelSplit) split);
	}

	@Override
	public InputSplit[] getSplits(JobConf conf,
			int arg1) throws IOException {
		logger.info("_______________________________________getSplits");
		logger.info(String.format("hive.excel.file.path=%s", conf.get("hive.excel.file.path")));
		logger.info(String.format("hive.excel.sheet.name=%s", conf.get("hive.excel.sheet.name")));
		logger.info(String.format("hive.excel.sheet.index=%s", conf.get("hive.excel.sheet.index")));

		//Configuration conf = context.getConfiguration();
		String filepath = conf.get("hive.excel.file.path");
		Path path = new Path(filepath);
		FileSystem fileSystem = path.getFileSystem(conf);
		FileStatus fileStatus = fileSystem.getFileStatus(path);

		//		long length = fileStatus.getLen();
		//		BlockLocation[] blkLocations = path.getFileSystem(conf).getFileBlockLocations(fileStatus, 0, length);
		String[] splitHosts = null;
		//getSplitHosts(blkLocations, 0, length, new NetworkTopology());
		ExcelSplit split = new ExcelSplit(path, 0, fileStatus.getLen(), splitHosts);

		List<InputSplit> splits = new ArrayList<InputSplit>(1);
		splits.add(split);

		return splits.toArray(new InputSplit[1]);
	}
}
