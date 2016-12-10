package com.xukaiqiang.builder;

import java.util.List;

import com.xukaiqiang.bean.Column;
import com.xukaiqiang.bean.DataDTO;
import com.xukaiqiang.util.PropertyUtils;

/**
 * @ClassName: Builder
 * @Description: 读取要生成的表，给生成代码的表里插入数据
 * @author xukaiqiang
 * @date 2016年12月10日 下午4:12:05
 * @modifier
 * @modify-date 2016年12月10日 下午4:12:05
 * @version 1.0
*/
	
public class Builder {
	public static void main(String[] args) {
		//读取配置文件，读取要生成的表
		String tableDB_Name=PropertyUtils.getValue("table_db_Name", "db.properties");
		String table_Name=PropertyUtils.getValue("table_name", "db.properties");
		String java_name=PropertyUtils.getValue("java_name", "db.properties");
		//获取表的名称
		//插入表记录
		DBHelper dbHelper=new DBHelper();
		DataDTO dataDTO=new DataDTO(tableDB_Name, java_name, "cb", "table", table_Name);
		//判断表中的注释是否有空的
		List<Column> columns=dbHelper.queryColumns(tableDB_Name);
		//数据库字段是否都有注释了
		boolean  remarkIsOk=true;
		for (Column column : columns) {
			if("".equals(column.getRemark())||null==column.getRemark()){
				remarkIsOk=false;
				System.err.println(column.getName()+"在数据库表("+tableDB_Name+")中没有注释，请插入后重试！！");
			}
		}
		
		if (remarkIsOk) {
			//判断表是否已经插入
			boolean isSaved=dbHelper.isSaved(dataDTO.getDb_name());
			if (isSaved) {
				dbHelper.insertTableRecord(dataDTO);
				//获取表中的字段
				//插入字段记录
				dbHelper.insertFieldRecord(tableDB_Name);
			}else{
				System.err.println("表("+tableDB_Name+")内容已经插入了........");
			}
		}
	}
}
