package com.nexr.hive.excel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelSerDe extends AbstractSerDe {
	private static final Logger logger = LoggerFactory.getLogger(ExcelSerDe.class);

	private final ExcelRowWritable row = new ExcelRowWritable();

	private List<String> columnNames;
	private List<TypeInfo> columnTypes;

	private StructTypeInfo structTypeInfo;
	private ObjectInspector objectInspector;

	@Override
	public List<String> deserialize(Writable writable) throws SerDeException {
		logger.debug("___________________________________deserialize");
		logger.debug("writable : " + writable);

		//		List<String> values = new ArrayList<String>();
		//		for (Cell cell : ((ExcelRowWritable) writable).get()) {
		//			cell.setCellType(Cell.CELL_TYPE_STRING);
		//			values.add(cell.getStringCellValue());
		//		}
		return ((ExcelRowWritable) writable).get();

		//		if (writable instanceof ExcelRowWritable) {
		//			return ((ExcelRowWritable) writable).get();
		//		} else {
		//			throw new SerDeException("Expected ExcelRowWritable, received " + writable.getClass().getName());
		//		}
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public void initialize(Configuration configuration, Properties properties)
			throws SerDeException {
		logger.debug("___________________________________initialize");

		String columnName = properties.getProperty(Constants.LIST_COLUMNS);
		String columnType = properties.getProperty(Constants.LIST_COLUMN_TYPES);
		logger.debug(String.format("columnName : %s, columnType : %s", columnName, columnType));

		if (StringUtils.isNotEmpty(columnName)) {
			columnNames = Arrays.asList(columnName.split(","));
		} else {
			columnNames = new ArrayList<String>();
		}

		if (StringUtils.isNotEmpty(columnType)) {
			columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnType);
		} else {
			columnTypes = new ArrayList<TypeInfo>();
		}

		PrimitiveCategory[] columnTypes = toTypes(columnType.split(":"));
		PrimitiveObjectInspector[] columnOIsArray = toPrimitiveJavaOIs(columnTypes);
		objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames,
				new ArrayList<ObjectInspector>(Arrays.asList(columnOIsArray)));

		//structTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, this.columnTypes);
		//objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(structTypeInfo);

	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return ExcelRowWritable.class;
	}

	@Override
	public Writable serialize(Object row, ObjectInspector inspector)
			throws SerDeException {
		return this.row;
	}

	private static PrimitiveCategory[] toTypes(String[] types) {
		PrimitiveCategory[] categories = new PrimitiveCategory[types.length];
		for (int i = 0; i < types.length; i++) {
			PrimitiveObjectInspectorUtils.PrimitiveTypeEntry entry =
					PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(types[i]);
			if (entry == null) {
				throw new IllegalArgumentException("Invalid primitive type " + types[i]);
			}
			categories[i] = entry.primitiveCategory;
		}
		return categories;
	}

	private static PrimitiveObjectInspector[] toPrimitiveJavaOIs(PrimitiveCategory[] categories) {
		PrimitiveObjectInspector[] inspectors = new PrimitiveObjectInspector[categories.length];
		for (int i = 0; i < categories.length; i++) {
			inspectors[i] = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(categories[i]);
		}
		return inspectors;
	}

}
