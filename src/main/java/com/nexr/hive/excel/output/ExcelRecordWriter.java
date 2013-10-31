package com.nexr.hive.excel.output;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelRecordWriter implements RecordWriter {
	private static final Logger logger = LoggerFactory.getLogger(ExcelRecordWriter.class);

	private OPCPackage pkg;
	private Workbook workbook;
	private Sheet sheet;

	private long pos;

	FSDataOutputStream fos;

	public ExcelRecordWriter() throws IOException {
		this(null, null);
	}

	public ExcelRecordWriter(JobConf conf, Path path) throws IOException {
		String filepath = conf.get("hive.excel.file.path");
		String sheetName = "__hive_result__";

		FileSystem fs = path.getFileSystem(conf);
		fos = fs.create(path, true);

		pos = 0;

		try {
			pkg = OPCPackage.create(fos);
			workbook = new SXSSFWorkbook(new XSSFWorkbook(pkg), 100);

			sheet = workbook.getSheet(sheetName);
			if (sheet == null) {
				sheet = workbook.createSheet(sheetName);
			}
		} catch (Exception e) {
			logger.error(null, e);
			throw new IOException(e);
		}
	}

	@Override
	public void close(boolean arg0) throws IOException {
		workbook.write(fos);
		fos.close();

		if (pkg != null) {
			pkg.close();
		}
	}

	@Override
	public void write(Writable writable) throws IOException {
		if (!(writable instanceof Text)) {
			throw new IOException("writable is not Text.");
		}

		Text text = (Text) writable;
		Row row = sheet.createRow((int) pos);
		Cell cell = row.createCell(0, Cell.CELL_TYPE_STRING);
		cell.setCellValue(text.toString());

		++pos;
		//text.
	}
}
