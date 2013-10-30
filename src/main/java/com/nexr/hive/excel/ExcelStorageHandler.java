package com.nexr.hive.excel;

import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
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

import com.nexr.hive.excel.input.ExcelInputFormat;


public class ExcelStorageHandler extends DefaultStorageHandler implements HiveMetaHook {
	private static final Logger logger = LoggerFactory.getLogger(ExcelStorageHandler.class);

	private Configuration conf;

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
		return this;
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {

		for(Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()){
			logger.info(String.format("input job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {

		for(Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()){
			logger.info(String.format("output job properties = %s : %s", entry.getKey(), entry.getValue()));
			jobProperties.put(ObjectUtils.toString(entry.getKey()), ObjectUtils.toString(entry.getValue()));
		}
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {

		for(Map.Entry<Object, Object> entry : tableDesc.getProperties().entrySet()){
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
	public void commitCreatePartition(
			org.apache.hadoop.hive.metastore.api.Table arg0, Partition arg1)
					throws MetaException {
	}

	@Override
	public void commitCreateTable(
			org.apache.hadoop.hive.metastore.api.Table arg0)
					throws MetaException {
	}

	@Override
	public void commitDropPartition(
			org.apache.hadoop.hive.metastore.api.Table arg0, Partition arg1,
			boolean arg2) throws MetaException {
	}

	@Override
	public void commitDropTable(
			org.apache.hadoop.hive.metastore.api.Table arg0, boolean arg1)
					throws MetaException {
	}

	@Override
	public void preCreatePartition(
			org.apache.hadoop.hive.metastore.api.Table arg0, Partition arg1)
					throws MetaException {
	}

	@Override
	public void preCreateTable(org.apache.hadoop.hive.metastore.api.Table arg0)
			throws MetaException {
	}

	@Override
	public void preDropPartition(
			org.apache.hadoop.hive.metastore.api.Table arg0, Partition arg1)
					throws MetaException {
	}

	@Override
	public void preDropTable(org.apache.hadoop.hive.metastore.api.Table arg0)
			throws MetaException {
	}

	@Override
	public void rollbackCreatePartition(
			org.apache.hadoop.hive.metastore.api.Table arg0, Partition arg1)
					throws MetaException {
	}

	@Override
	public void rollbackCreateTable(
			org.apache.hadoop.hive.metastore.api.Table arg0)
					throws MetaException {
	}

	@Override
	public void rollbackDropPartition(
			org.apache.hadoop.hive.metastore.api.Table arg0, Partition arg1)
					throws MetaException {
	}

	@Override
	public void rollbackDropTable(
			org.apache.hadoop.hive.metastore.api.Table arg0)
					throws MetaException {
	}
}
