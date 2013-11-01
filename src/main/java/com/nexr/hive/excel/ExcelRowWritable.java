package com.nexr.hive.excel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class ExcelRowWritable implements Writable {

	private final List<String> row = new ArrayList<String>();

	public void add(String cell) {
		row.add(cell);
	}

	public List<String> get() {
		return row;
	}

	public void clear() {
		row.clear();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		throw new UnsupportedOperationException("write");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		throw new UnsupportedOperationException("readFields");
	}

	@Override
	public String toString() {
		return row.toString();
	}

}
