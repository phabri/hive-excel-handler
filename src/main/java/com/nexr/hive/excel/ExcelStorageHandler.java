package com.nexr.hive.excel;

import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExcelStorageHandler extends DefaultStorageHandler {
	private static final Logger logger = LoggerFactory.getLogger(ExcelStorageHandler.class);
	
	@Override
	public boolean supports(Table tbl, AlterTableTypes alter) {
	    return
	            alter == AlterTableDesc.AlterTableTypes.ADDPROPS ||
	            alter == AlterTableDesc.AlterTableTypes.DROPPROPS ||
	            alter == AlterTableDesc.AlterTableTypes.ADDSERDEPROPS;
		
	    //return super.supports(tbl, alter);
	}
	
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return ExcelInputFormat.class;
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return super.getOutputFormatClass();
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return super.getSerDeClass();
	}
	
	@Override
	public HiveMetaHook getMetaHook() {
		return super.getMetaHook();
	}
	
	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		for(Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()){
			logger.debug(String.format("input job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
		//super.configureInputJobProperties(tableDesc, jobProperties);
	}
	
	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		for(Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()){
			logger.debug(String.format("output job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
		//super.configureOutputJobProperties(tableDesc, jobProperties);
	}
	
	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		for(Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()){
			logger.debug(String.format("table job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
		//super.configureTableJobProperties(tableDesc, jobProperties);
	}
}
