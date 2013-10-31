package com.nexr.hive.excel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelSerDe implements SerDe {
	private static final Logger logger = LoggerFactory.getLogger(ExcelSerDe.class);

	private final ExcelRowWritable row = new ExcelRowWritable();

	private List<String> columnNames;
	private List<TypeInfo> columnTypes;

	private ObjectInspector objectInspector;

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		if (writable instanceof ExcelRowWritable) {
			return ((ExcelRowWritable) writable).get();
		} else {
			throw new SerDeException("Expected ExcelRowWritable, received " + writable.getClass().getName());
		}
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

		//		StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
		//		List<String> fieldNames = typeInfo.getAllStructFieldNames();
		//		List<TypeInfo> fieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();
		//		List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fieldTypeInfos.size());
		//		for (int i = 0; i < fieldTypeInfos.size(); i++) {
		//			fieldObjectInspectors.add(getJsonObjectInspectorFromTypeInfo(fieldTypeInfos.get(i), null));
		//		}
		//		objectInspector = JsonObjectInspectorFactory.getJsonStructObjectInspector(fieldNames,
		//				fieldObjectInspectors, options);

		PrimitiveCategory[] columnTypes = toTypes(columnType.split(":"));
		PrimitiveObjectInspector[] columnOIsArray = toPrimitiveJavaOIs(columnTypes);
		objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
				columnNames, new ArrayList<ObjectInspector>(Arrays.asList(columnOIsArray)));
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return ExcelRowWritable.class;
	}

	@Override
	public Writable serialize(Object row, ObjectInspector inspector)
			throws SerDeException {
		return null;
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
			inspectors[i] =
					PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(categories[i]);
		}
		return inspectors;
	}

}
