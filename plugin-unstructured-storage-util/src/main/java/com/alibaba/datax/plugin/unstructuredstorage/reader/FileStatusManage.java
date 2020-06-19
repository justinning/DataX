package com.alibaba.datax.plugin.unstructuredstorage.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStatusManage {
	private static final Logger LOG = LoggerFactory.getLogger(FileStatusManage.class);
	private String JDBC_URL;
	private String DRIVER_CLASS;
	private String USER;
	private String PASSWORD;
	private boolean bDbReady = false;
	private final String TABLE_NAME = "datax_file_status";

	public FileStatusManage() {
		String dataxHome = System.getProperty("datax.home");

		if (dataxHome != null && !"".equals(dataxHome)) {
			Properties prop = new Properties();
			try {
				String propFile = dataxHome + File.separatorChar + "conf" + File.separatorChar + "ext.conf";
				prop.load(new FileInputStream(propFile));
				JDBC_URL = prop.getProperty("ext.jdbcUrl");
				DRIVER_CLASS = prop.getProperty("ext.driverClass");
				USER = prop.getProperty("ext.userName");
				PASSWORD = prop.getProperty("ext.password");
			} catch (IOException e) {
			}
		}

		if (JDBC_URL == null || DRIVER_CLASS == null || USER == null || PASSWORD == null) {
			LOG.error("状态数据库JDBC信息不完整，请检查！");
			return;
		}

		try {
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			LOG.error(String.format("JDBC驱动没找到 [ %s ]", DRIVER_CLASS));
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
						"CREATE TABLE %s(id VARCHAR(50) PRIMARY KEY, fileserver_id VARCHAR(50) NOT NULL, path VARCHAR(512) NOT NULL, last_modified VARCHAR(20) NOT NULL, update_time datetime NOT NULL)",
						TABLE_NAME));
			}
			
			bDbReady = true;
			statement.close();
			conn.close();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
	}

	public boolean isNewFile(String fileserver_id, FileInfo info, String readMode) {
		if (Constant.READ_FULL.equals(readMode) || !bDbReady) {
			return true;
		}

		boolean result = true;
		String id = md5(fileserver_id.toLowerCase() + info.getPath().toLowerCase());
		String strTime = formatTimeString(info.getLastModified(), null);
		
		try {
			Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement statement = conn.createStatement();
			// host和路径转成小写，让路径不区分大小写

			String strSql = String.format("SELECT last_modified FROM %s WHERE id='%s'", TABLE_NAME, id);
			ResultSet rs = statement.executeQuery(strSql);
			if (rs.first()) {
				if (Constant.READ_DIFFERENT_NAME.equals(readMode)) {
					result = false;
			    } else if (Constant.READ_DIFFERENT.equals(readMode) && strTime.equals(rs.getString("last_modified"))) {
					result = false;
				} else if (Constant.READ_LATEST.equals(readMode)
						&& strTime.compareTo(rs.getString("last_modified")) <= 0) {
					result = false;
				}
			}
			statement.close();
			conn.close();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
		LOG.info(String.format("新文件判断 id=%s,serverId=%s,path=%s,last_modified=%s,result=%b", 
				id,fileserver_id.toLowerCase(),info.getPath().toLowerCase(),strTime,result));
		return result;
	}

	public void updateStatus(String fileserver_id, FileInfo info) {
		if (!bDbReady) {
			return;
		}

		try {
			Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement statement = conn.createStatement();

			String strTime = formatTimeString(info.getLastModified(), null);
			// host和路径转成小写，让路径不区分大小写
			String id = md5(fileserver_id.toLowerCase() + info.getPath().toLowerCase());
			String strSql = String.format("SELECT last_modified FROM %s WHERE id='%s'", TABLE_NAME, id);
			ResultSet rs = statement.executeQuery(strSql);
			String nowTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
			
			if (rs.first()) {
				strSql = String.format("UPDATE %s set last_modified='%s',update_time='%s' WHERE id='%s'", TABLE_NAME, strTime,nowTime, id);
			} else {
				strSql = String.format("INSERT INTO %s (id,fileserver_id,path,last_modified,update_time) values('%s','%s','%s','%s','%s')",
						TABLE_NAME, id, fileserver_id, info.getPath(), strTime,nowTime);
			}
			statement.executeUpdate(strSql);
			statement.close();
			conn.close();
			LOG.info(String.format("更新文件记录 id=%s,serverId=%s,path=%s,last_modified=%s", 
					id,fileserver_id.toLowerCase(),info.getPath().toLowerCase(),strTime));
		} catch (SQLException e) {
			LOG.error(e.getMessage());
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
			LOG.error(e.getMessage());
		}
		return sb.toString();
	}
}
