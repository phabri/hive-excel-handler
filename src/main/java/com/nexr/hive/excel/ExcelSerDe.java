package com.nexr.hive.excel;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

public class ExcelSerDe implements SerDe {

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		if(writable instanceof ObjectWritable){
			Object o = ((ObjectWritable) writable).get();
			if(o instanceof List<?>){
				
			}
		}
		return null;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initialize(Configuration configuration, Properties properties)
			throws SerDeException {
	    String columns = properties.getProperty(ConfigurationUtils.LIST_COLUMNS);
	    String types = properties.getProperty(ConfigurationUtils.LIST_COLUMN_TYPES);
		
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1)
			throws SerDeException {
		return null;
	}

}
