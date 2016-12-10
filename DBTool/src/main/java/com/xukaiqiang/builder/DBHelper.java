package com.xukaiqiang.builder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.xukaiqiang.bean.Column;
import com.xukaiqiang.bean.DataDTO;
import com.xukaiqiang.util.PropertyUtils;
import com.xukaiqiang.util.StringUtil;

/**
 * @ClassName: DBHelper
 * @Description: 操作数据库
 * @author xukaiqiang
 * @date 2016年12月10日 下午4:32:12
 * @modifier
 * @modify-date 2016年12月10日 下午4:32:12
 * @version 1.0
 */

public class DBHelper {

	/**
	 * Class Name: DBHelper.java
	 * 
	 * @Description: 插入表记录
	 * @author xukaiqiang
	 * @date 2016年12月10日 下午4:35:58
	 * @modifier
	 * @modify-date 2016年12月10日 下午4:35:58
	 * @version 1.0
	 * @param dataDTO
	 */
	public void insertTableRecord(DataDTO dataDTO) {
		try {
			Connection connection=getConn();
			String sql = "insert into dbre_metadata (db_name,java_name,parent_name,data_type,remarks) values(?,?,?,?,?)";
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setString(1, dataDTO.getDb_name());
			statement.setString(2, dataDTO.getJava_name());
			statement.setString(3, dataDTO.getParent_name());
			statement.setString(4,	dataDTO.getData_type());
			statement.setString(5,	dataDTO.getRemarks());
			int i=statement.executeUpdate();
			System.out.println(dataDTO.getRemarks()+"插入成功");
			closeConn(connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	
	/**
	 * Class Name: DBHelper.java
	 * @Description: 判断表是否已经插入
	 * @author xukaiqiang
	 * @date 2016年12月10日 下午5:38:35
	 * @modifier
	 * @modify-date 2016年12月10日 下午5:38:35
	 * @version 1.0
	 * @param db_name
	 * @return
	*/
		
	public boolean  isSaved(String db_name){
		try {
			Connection connection=getConn();
			String sql = "select db_name from dbre_metadata  where   db_name='" + db_name+"'";
			ResultSet resultSet=createQuery(connection,sql);
			if (resultSet.next()) {
				return false;
			}
			resultSet.close();
			closeConn(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	
	/**
	 * Class Name: DBHelper.java
	 * @Description:插入字段记录
	 * @author xukaiqiang
	 * @date 2016年12月10日 下午4:46:40
	 * @modifier
	 * @modify-date 2016年12月10日 下午4:46:40
	 * @version 1.0
	 * @param tableName
	*/
	public void insertFieldRecord(String tableName){
		List<Column> columns=queryColumns(tableName);
		for (Column column : columns) {
			//获取所有的字段
			DataDTO dataDTO=new DataDTO();
			dataDTO.setData_type("field");
			dataDTO.setDb_name(column.getName());
			dataDTO.setParent_name(tableName);
			dataDTO.setJava_name(column.getNameJ());
			dataDTO.setRemarks(column.getRemark());
			insertTableRecord(dataDTO);
			System.out.println(column.getRemark()+"插入成功");
		}
		
	}
	
	
	/**
	 * Class Name: DBHelper.java
	 * @Description: 查询表中所有的字段
	 * @author xukaiqiang
	 * @date 2016年12月10日 下午5:24:12
	 * @modifier
	 * @modify-date 2016年12月10日 下午5:24:12
	 * @version 1.0
	 * @param tableName
	 * @return
	*/
	public  List<Column> queryColumns(String tableName) {
		List<Column> list = new ArrayList<Column>();
		try {
			Connection conn = getConn();
			ResultSet rs = createQuery(conn, "SELECT col_description(a.attrelid,a.attnum) as comment,format_type(a.atttypid,a.atttypmod) as type,a.attname as name, a.attnotnull as notnull  FROM pg_class as c,pg_attribute as a  where c.relname = '"+tableName+"' and a.attrelid = c.oid and a.attnum>0");
			while (rs.next()) {
				String javaStyle = StringUtil.javaStyle(rs.getString(3));
				String name=rs.getString(3);
				list.add(new Column(name, javaStyle, rs.getString(1)));
			}
			rs.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	/**
	 * Class Name: DBHelper.java
	 * @Description: 创建查询
	 * @author xukaiqiang
	 * @date 2016年12月10日 下午4:52:23
	 * @modifier
	 * @modify-date 2016年12月10日 下午4:52:23
	 * @version 1.0
	 * @param conn
	 * @param sql
	 * @return
	 * @throws SQLException
	*/
	public ResultSet createQuery(Connection conn, String sql) throws SQLException {
		return conn.createStatement().executeQuery(sql);
	}
	/**
	 * Class Name: DBHelper.java
	 * 
	 * @Description:获取数据库连接
	 * @author xukaiqiang
	 * @date 2016年12月10日 下午4:36:26
	 * @modifier
	 * @modify-date 2016年12月10日 下午4:36:26
	 * @version 1.0
	 * @return
	 */
	public Connection getConn() {
		String url=PropertyUtils.getValue("url", "db.properties");
		String userName=PropertyUtils.getValue("username", "db.properties");
		String passWord=PropertyUtils.getValue("password", "db.properties");
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(url,userName,passWord);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connection;
	}
	
	/**
	 * Class Name: DBHelper.java
	 * @Description:关闭数据库连接
	 * @author xukaiqiang
	 * @date 2016年12月10日 下午4:38:32
	 * @modifier
	 * @modify-date 2016年12月10日 下午4:38:32
	 * @version 1.0
	 * @param connection
	 * @return
	*/
		
	public  void closeConn(Connection connection){
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
