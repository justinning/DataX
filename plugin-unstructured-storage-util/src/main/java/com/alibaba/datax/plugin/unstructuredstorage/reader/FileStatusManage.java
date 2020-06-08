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
						"CREATE TABLE %s(id VARCHAR(50) PRIMARY KEY, host VARCHAR(50) NOT NULL, path VARCHAR(512) NOT NULL, last_modified VARCHAR(20) NOT NULL)",
						TABLE_NAME));
			}
			statement.close();
			conn.close();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
	}

	public boolean isNewFile(String host, FileInfo info, String readMode) {
		if (Constant.READ_FULL.equals(readMode) || !bDbReady) {
			return true;
		}

		boolean result = true;

		try {
			Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement statement = conn.createStatement();
			String strTime = formatTimeString(info.getLastModified(), null);
			// host和路径转成小写，让路径不区分大小写
			String id = md5(host.toLowerCase() + info.getPath().toLowerCase());
			String strSql = String.format("SELECT last_modified FROM %s WHERE id='%s'", TABLE_NAME, id);
			ResultSet rs = statement.executeQuery(strSql);
			if (rs.first()) {

				if (Constant.READ_DIFFERENT.equals(readMode) && strTime.equals(rs.getString("last_modified"))) {
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
		return result;
	}

	public void updateStatus(String host, FileInfo info) {
		if (!bDbReady) {
			return;
		}

		try {
			Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement statement = conn.createStatement();

			String strTime = formatTimeString(info.getLastModified(), null);
			// host和路径转成小写，让路径不区分大小写
			String id = md5(host.toLowerCase() + info.getPath().toLowerCase());
			String strSql = String.format("SELECT last_modified FROM %s WHERE id='%s'", TABLE_NAME, id);
			ResultSet rs = statement.executeQuery(strSql);
			if (rs.first()) {
				strSql = String.format("UPDATE %s set last_modified='%s' WHERE id='%s'", TABLE_NAME, strTime, id);
			} else {
				strSql = String.format("INSERT INTO %s (id,host,path,last_modified) values('%s','%s','%s','%s')",
						TABLE_NAME, id, host, info.getPath(), strTime);
			}
			statement.executeUpdate(strSql);
			statement.close();
			conn.close();

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
