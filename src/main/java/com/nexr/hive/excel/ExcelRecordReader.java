package com.nexr.hive.excel;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class ExcelRecordReader implements RecordReader<LongWritable, Text>{
	private static final Logger logger = LoggerFactory.getLogger(ExcelRecordReader.class);
	
	private OPCPackage pkg;
	private Workbook workbook;
	private Sheet sheet;
	
	private long max;
	private long pos;

	public ExcelRecordReader(JobConf conf, ExcelSplit split) throws IOException{
		logger.error("_______________________________________getRecordReader");

		//String filepath = conf.get("hive.excel.file.path");		
		String sheetName = conf.get("hive.excel.sheet.name");		
		Integer sheetIndex = conf.getInt("hive.excel.sheet.index", 0);		

		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream fis = fs.open(path);
		
		try{
			pkg = OPCPackage.open(fis);
			workbook = new XSSFWorkbook(pkg);
		}catch(Exception e){
			logger.error(null, e);
		}
		
		if(StringUtils.isNotEmpty(sheetName)){
			sheet = workbook.getSheet(sheetName);
		}else{
			sheet = workbook.getSheetAt(sheetIndex);
		}
		
		pos = 0;
		max = sheet.getLastRowNum();
	}

	@Override
	public void close() throws IOException {
		if(pkg!=null) {
			pkg.close();
		}
	}

	@Override
	public float getProgress() throws IOException {
		return max==0 ? 1.0f : (float)pos / (float)max;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		Row row = sheet.getRow((int)pos);
		if(row==null){
			return false;
		}
		
		StringBuffer sb = new StringBuffer();
		for(Cell cell : row){
			cell.setCellType(Cell.CELL_TYPE_STRING);
			sb.append(cell.getStringCellValue());
			sb.append("\t");
		}
		
		key.set(pos);
		value.set(sb.toString());

		++pos;
		return true;
	}

}
