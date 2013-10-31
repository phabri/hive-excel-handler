package com.nexr.hive.excel;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.poi.ss.usermodel.Cell;

public class ExcelObjectInspect extends StandardStructObjectInspector {

	protected ExcelObjectInspect(List<String> structFieldNames,
			List<ObjectInspector> structFieldObjectInspectors,
			List<String> structFieldComments) {
		super(structFieldNames, structFieldObjectInspectors, structFieldComments);
	}

	@Override
	public Object getStructFieldData(Object data, StructField fieldRef) {
		if (data == null) {
			return null;
		}

		List<Cell> cells = (List<Cell>) data;
		MyField field = (MyField) fieldRef;
		int fieldIndex = field.getFieldID();

		Cell cell = cells.get(fieldIndex);
		cell.setCellType(Cell.CELL_TYPE_STRING);
		return cell.getStringCellValue();
	}

	@Override
	public List<Object> getStructFieldsDataAsList(Object data) {
		List<Object> values = new ArrayList<Object>();
		List<Cell> cells = (List<Cell>) data;

		for (Cell cell : cells) {
			cell.setCellType(Cell.CELL_TYPE_STRING);
			values.add(cell.getStringCellValue());
		}
		return values;
	}
}
