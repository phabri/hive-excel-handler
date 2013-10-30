package com.nexr.hive.excel;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;


public class ExcelStorageHandler extends DefaultStorageHandler {
	
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
		// TODO Auto-generated method stub
		return super.getOutputFormatClass();
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		// TODO Auto-generated method stub
		return super.getSerDeClass();
	}
	
	@Override
	public HiveMetaHook getMetaHook() {
		return super.getMetaHook();
	}
}
