package com.nexr.hive.excel.input;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.hive.excel.ExcelRowWritable;
import com.nexr.hive.excel.ExcelSplit;

public class ExcelRecordReader implements RecordReader<LongWritable, ExcelRowWritable> {
	private static final Logger logger = LoggerFactory.getLogger(ExcelRecordReader.class);

	private OPCPackage pkg;
	private Workbook workbook;
	private Sheet sheet;

	private long max;
	private long pos;

	public ExcelRecordReader(JobConf conf, ExcelSplit split) throws IOException {
		logger.info("_______________________________________ExcelRecordReader");

		String filepath = conf.get("hive.excel.file.path");
		String sheetName = conf.get("hive.excel.sheet.name");
		Integer sheetIndex = conf.getInt("hive.excel.sheet.index", 0);
		//conf.set("map.input.file", filepath);

		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream fis = fs.open(path);

		try {
			pkg = OPCPackage.open(fis);
			workbook = new XSSFWorkbook(pkg);
		} catch (Exception e) {
			logger.error(null, e);
		}

		if (StringUtils.isNotEmpty(sheetName)) {
			sheet = workbook.getSheet(sheetName);
		} else {
			sheet = workbook.getSheetAt(sheetIndex);
		}

		pos = 0;
		max = sheet.getLastRowNum();
	}

	@Override
	public void close() throws IOException {
		if (pkg != null) {
			pkg.close();
		}
	}

	@Override
	public float getProgress() throws IOException {
		return max == 0 ? 1.0f : (float) pos / (float) max;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public ExcelRowWritable createValue() {
		return new ExcelRowWritable();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public boolean next(LongWritable key, ExcelRowWritable value) throws IOException {
		Row row = sheet.getRow((int) pos);
		if (row == null) {
			return false;
		}

		key.set(pos);

		for (Cell cell : row) {
			cell.setCellType(Cell.CELL_TYPE_STRING);
			value.add(cell);
		}

		//logger.debug("value : " + value.toString());

		++pos;
		return true;
	}

}
