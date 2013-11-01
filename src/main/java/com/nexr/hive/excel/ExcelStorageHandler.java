package com.nexr.hive.excel;

import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.hive.excel.input.ExcelInputFormat;
import com.nexr.hive.excel.output.ExcelOutputFormat;

public class ExcelStorageHandler implements HiveStorageHandler {
	private static final Logger logger = LoggerFactory.getLogger(ExcelStorageHandler.class);

	private Configuration conf;

	@Override
	public boolean supports(Table tbl, AlterTableTypes alter) {
		return alter == AlterTableDesc.AlterTableTypes.ADDPROPS ||
				alter == AlterTableDesc.AlterTableTypes.DROPPROPS ||
				alter == AlterTableDesc.AlterTableTypes.ADDSERDEPROPS;

		//return super.supports(tbl, alter);
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		logger.debug("_____________________________________getInputFormatClass");

		return ExcelInputFormat.class;
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		logger.debug("_____________________________________getOutputFormatClass");

		return ExcelOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		logger.debug("_____________________________________getSerDeClass");

		return ExcelSerDe.class;
		//return super.getSerDeClass();
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return null;
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {

		for (Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()) {
			logger.info(String.format("input job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {

		for (Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()) {
			logger.info(String.format("output job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {

		for (Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()) {
			logger.info(String.format("table job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
		return new DefaultHiveAuthorizationProvider();
	}

}
