package com.alibaba.datax.plugin.unstructuredstorage.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @see CREATE TABLE SCRIPT
 * CREATE TABLE datax_file_status(
 *    id VARCHAR(50) PRIMARY KEY,
 *    host VARCHAR(50) NOT NULL, 
 *    path VARCHAR(512) NOT NULL, 
 *    last_modified VARCHAR(20) NOT NULL );
 */
public class FileStatusManage {

	private static String JDBC_URL = "jdbc:h2:file:./h2DB";
	private static String DRIVER_CLASS = "org.h2.Driver";
	private static String USER = "root";
	private static String PASSWORD = "root";

	private final String TABLE_NAME = "datax_file_status";
	
	public FileStatusManage() {
		String dataxHome = System.getProperty("datax.home");

		if (dataxHome != null && !"".equals(dataxHome)) {
			Properties prop = new Properties();
			try {
				String propFile = dataxHome + File.separatorChar + "conf" + File.separatorChar + "ext.conf";
				prop.load(new FileInputStream(propFile));
				JDBC_URL = prop.getProperty("ext.jdbcUrl", JDBC_URL);
				DRIVER_CLASS = prop.getProperty("ext.driverClass", DRIVER_CLASS);
				USER = prop.getProperty("ext.userName", USER);
				PASSWORD = prop.getProperty("ext.password", PASSWORD);
			} catch (IOException e) {
			}
		}

		try {
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		try {
			// 与数据库建立连接测试用户名密码是否正确
			Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement statement = conn.createStatement();
			String strSql = String.format("SELECT * FROM %s WHERE 1=2", TABLE_NAME);
			try {
				statement.executeQuery(strSql);
			} catch (SQLException e) {
				// 表不存在，创建表
				statement.execute(String.format(
						"CREATE TABLE %s(id VARCHAR(50) PRIMARY KEY, host VARCHAR(50) NOT NULL, path VARCHAR(512) NOT NULL, last_modified VARCHAR(20) NOT NULL)",
						TABLE_NAME));
			}
			statement.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public boolean isNewFile(String host, FileInfo info, String readMode) {
		if (Constant.READ_FULL.equals(readMode)) {
			return true;
		}
		
		boolean result = true;
		
		try {
			Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement statement = conn.createStatement();
			String strTime = formatTimeString(info.getLastModified(), null);
			String id = md5(host + info.getPath());
			String strSql = String.format("SELECT last_modified FROM %s WHERE id='%s'", TABLE_NAME, id);
			ResultSet rs = statement.executeQuery(strSql);
			if (rs.first()) {
				
				if (Constant.READ_DIFFERENT.equals(readMode) && strTime.equals(rs.getString("last_modified"))) {
					result = false;
				} else if (Constant.READ_LATEST.equals(readMode) && strTime.compareTo(rs.getString("last_modified")) <=0 ){
					result = false;
				}
			}
			statement.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public void updateStatus(String host, FileInfo info) {
		try {
			Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement statement = conn.createStatement();
			System.out.println(info.getPath()+","+ info.getLastModified());
			String strTime = formatTimeString(info.getLastModified(), null);
			String id = md5(host + info.getPath());
			String strSql = String.format("SELECT last_modified FROM %s WHERE id='%s'", TABLE_NAME, id);
			ResultSet rs = statement.executeQuery(strSql);
			if (rs.first()) {
				strSql = String.format("UPDATE %s set last_modified='%s' WHERE id='%s'", TABLE_NAME,strTime,id);				
			} else {
				strSql = String.format("INSERT INTO %s (id,host,path,last_modified) values('%s','%s','%s','%s')",
						TABLE_NAME,id,host,info.getPath(),strTime);
			}
			statement.executeUpdate(strSql);
			statement.close();
			conn.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * TimeZone.getTimeZone("GMT+:08:00")
	 */
	private String formatTimeString(long nTime, TimeZone timeZone) {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if (timeZone != null) {
			df.setTimeZone(timeZone);
		}
		Date d = new Date(nTime);
		return df.format(d);
	}

	private String md5(String value) {
		StringBuilder sb = new StringBuilder();
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("md5");
			byte[] bytes = messageDigest.digest(value.getBytes());
			for (int i = 0; i < bytes.length; i++) {
				int tempInt = bytes[i] & 0xff;
				if (tempInt < 16) {
					sb.append(0);
				}
				sb.append(Integer.toHexString(tempInt));
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		
		List<FileInfo> ls = new ArrayList<FileInfo>();
		
		FileStatusManage fs = new FileStatusManage();
		
		String filepath = "/Users/justin/Documents/Galaxeed/工作目录/数据样本/2020/2020.4月/20200412/ERP/erp-test.csv";
		long lastModifiedTime = new File(filepath).lastModified();
		try {
			InetAddress addr = InetAddress.getLocalHost();
			String host = addr.getHostName();
			FileInfo info = new FileInfo(filepath,lastModifiedTime);
			
			ls.add(info);
			String str = JSONObject.toJSONString(ls);
			List ls2 = (List) JSONObject.parseArray(str,FileInfo.class);
			for(Object o : ls2) {
				System.out.println(((FileInfo)o).getPath());
			}
			if(fs.isNewFile(host,info, null)) {
				fs.updateStatus(host, info);
			}
		} catch (UnknownHostException e) {
		}

	}

}
