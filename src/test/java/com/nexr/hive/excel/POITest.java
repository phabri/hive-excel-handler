package com.nexr.hive.excel;

import java.io.File;
import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class POITest {
	private static final Logger logger = LoggerFactory.getLogger(POITest.class);

	@Test
	public void test() throws InvalidFormatException, IOException{
		String filename = getClass().getResource("/excel/TMCIncident.xlsx").getFile();
		//String filename = getClass().getResource("/excel/WDI.xlsx").getFile();
		Assert.assertNotNull(filename);
		logger.info(filename);

		OPCPackage pkg = OPCPackage.open(new File(filename));
		Workbook wb = WorkbookFactory.create(new File(filename));
		Assert.assertNotNull(wb);

		Sheet sheet = wb.getSheetAt(0);
		int firstRowNum = sheet.getFirstRowNum();
		int lastRowNum = sheet.getLastRowNum();
		logger.info(String.format("sheet:%s range =  %d ~ %d", sheet.getSheetName(), firstRowNum, lastRowNum));
		
		for (Row row : sheet) {
			for (Cell cell : row) {
				cell.setCellType(Cell.CELL_TYPE_STRING);

				int rowIndex = cell.getRowIndex();
				int colIndex = cell.getColumnIndex();
				//logger.info(String.format("%d,%d : %s", rowIndex, colIndex, cell.getStringCellValue()));
			}
		}

		pkg.close();
	}
}
