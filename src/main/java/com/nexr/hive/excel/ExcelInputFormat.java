package com.nexr.hive.excel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;

public class ExcelInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader getRecordReader(InputSplit split, JobConf conf,
			Reporter reporter) throws IOException {
		if(!(split instanceof ExcelSplit)){
			throw new IOException("invalid input split");
		}
		
		return new ExcelRecordReader(conf, (ExcelSplit)split);
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
		FileStatus[] files = listStatus(conf);
		
		List<ExcelSplit> splits = new ArrayList<ExcelSplit>(files.length);
		
		for(FileStatus file : files){
			long length = file.getLen();
			BlockLocation[] blkLocations = file.getPath().getFileSystem(conf).getFileBlockLocations(file, 0, length);
			String[] splitHosts = getSplitHosts(blkLocations, 0, length, new NetworkTopology());
			 
			ExcelSplit split = new ExcelSplit(file.getPath(), 0, length, splitHosts);
			splits.add(split);
		}
		
		return splits.toArray(new ExcelSplit[files.length]);
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}
}
