package com.alibaba.datax.plugin.reader.ftpreader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.FileFormat;
import com.alibaba.datax.plugin.unstructuredstorage.reader.FileInfo;
import com.alibaba.datax.plugin.unstructuredstorage.reader.FileStatusManage;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson.JSONObject;

public class FtpReader extends Reader {
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration originConfig = null;

		private List<String> path = null;

		private HashSet<FileInfo> sourceFiles;

		// ftp链接参数
		private String protocol;
		private String host;
		private int port;
		private String username;
		private String password;
		private int timeout;
		private String connectPattern;
		private int maxTraversalLevel;
		
		private FtpHelper ftpHelper = null;
		private FileStatusManage statusManage = new FileStatusManage();
		
		@Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
			this.sourceFiles = new HashSet<FileInfo>();

			this.validateParameter();
			UnstructuredStorageReaderUtil.validateParameter(this.originConfig);
			
			if ("sftp".equals(protocol)) {
				//sftp协议
				this.port = originConfig.getInt(Key.PORT, Constant.DEFAULT_SFTP_PORT);
				this.ftpHelper = new SftpHelper();
			} else if ("ftp".equals(protocol)) {
				// ftp 协议
				this.port = originConfig.getInt(Key.PORT, Constant.DEFAULT_FTP_PORT);
				this.ftpHelper = new StandardFtpHelper();
			}		
			boolean bBinaryFile = originConfig.getBool(Key.BINARY_FILE,false);
			ftpHelper.loginFtpServer(host, username, password, port, timeout, connectPattern, bBinaryFile);

		}

		private void validateParameter() {
			//todo 常量
			this.protocol = this.originConfig.getNecessaryValue(Key.PROTOCOL, FtpReaderErrorCode.REQUIRED_VALUE);
			boolean ptrotocolTag = "ftp".equals(this.protocol) || "sftp".equals(this.protocol);
			if (!ptrotocolTag) {
				throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
						String.format("仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]", protocol));
			}
			this.host = this.originConfig.getNecessaryValue(Key.HOST, FtpReaderErrorCode.REQUIRED_VALUE);
			//优先从密码文件中读取用户名、密码
			String passwordFile = this.originConfig.getString(Key.PASSWORD_FILE);
			if( passwordFile != null && new File(passwordFile).exists()) {
				Properties p = new Properties();
				try {
					p.load(new FileInputStream(passwordFile));
					this.username = p.getProperty(Key.USERNAME);
					this.password = p.getProperty(Key.PASSWORD);
					
				} catch (Exception e) {
					System.out.println(e.getMessage());
					throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
							String.format("密码文件打开失败: [%s]", e.getMessage()));
				}
			}else {
				this.username = this.originConfig.getNecessaryValue(Key.USERNAME, FtpReaderErrorCode.REQUIRED_VALUE);
				this.password = this.originConfig.getNecessaryValue(Key.PASSWORD, FtpReaderErrorCode.REQUIRED_VALUE);
			}
			this.timeout = originConfig.getInt(Key.TIMEOUT, Constant.DEFAULT_TIMEOUT);
			this.maxTraversalLevel = originConfig.getInt(Key.MAXTRAVERSALLEVEL, Constant.DEFAULT_MAX_TRAVERSAL_LEVEL);
			
			// only support connect pattern
			this.connectPattern = this.originConfig.getUnnecessaryValue(Key.CONNECTPATTERN, Constant.DEFAULT_FTP_CONNECT_PATTERN, null);
			boolean connectPatternTag = "PORT".equals(connectPattern) || "PASV".equals(connectPattern);
			if (!connectPatternTag) {
				throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
						String.format("不支持您配置的ftp传输模式: [%s]", connectPattern));
			}else{
				this.originConfig.set(Key.CONNECTPATTERN, connectPattern);
			}

			//path check
			String pathInString = this.originConfig.getNecessaryValue(Key.PATH, FtpReaderErrorCode.REQUIRED_VALUE);
			if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
				path = new ArrayList<String>();
				path.add(pathInString);
			} else {
				path = this.originConfig.getList(Key.PATH, String.class);
				if (null == path || path.size() == 0) {
					throw DataXException.asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
				}
				for (String eachPath : path) {
					if(!eachPath.startsWith("/")){
						String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE, message);
					}
				}	
			}		

		}

		@Override
		public void prepare() {
			LOG.debug("prepare() begin...");
			boolean ignorePathNotExist = this.originConfig.getBool(Key.IGNORE_PATH_NOT_EXIST,false);
			
			try {
				HashSet<FileInfo> files = ftpHelper.getAllFiles(path, 0, maxTraversalLevel);
	
				// full,different,latest
				String readMode = this.originConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.READ_MODE, 
					com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.READ_FULL);
				String serverId = this.originConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FILESERVER_ID,"");
				String fileNameFilter = this.originConfig.getString(Key.PATH_FILTER);
				
				if( fileNameFilter == null || "".equals(fileNameFilter)){
					for(FileInfo file: files) {
						if(file.getSize() == 0) {
							LOG.error(String.format("[%s] 是一个空文件！", file.getPath()));
						}
						else if(isTargetFile(serverId,file,readMode)) {
							this.sourceFiles.add(file);
						}
					}
				}else {
					Pattern pat = Pattern.compile(fileNameFilter);				
					for(FileInfo file: files) {
						if(file.getSize() == 0) {
							LOG.error(String.format("[%s] 是一个空文件！", file.getPath()));
						}
						else if(pat.matcher(file.getPath()).matches() 
								&& isTargetFile(serverId, file, readMode)) {
							this.sourceFiles.add(file);
						}
					}
				}
			}catch (DataXException e) {
				if (e.getErrorCode() != FtpReaderErrorCode.FILE_NOT_EXISTS || !ignorePathNotExist)
					throw e;
			}
			LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
			if( this.sourceFiles.size() == 0 && !ignorePathNotExist) {
				LOG.warn(String.format("%s - %s 目录下没有匹配的文件",Thread.currentThread().getName(),this.path.toString()));
			}
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		    try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String.format(
                        "关闭与ftp服务器连接失败: [%s] host=%s, username=%s, port=%s",
                        e.getMessage(), host, username, port);
                LOG.error(message, e);
            }
		}

		// warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.debug("split() begin...");
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

			// warn:每个slice拖且仅拖一个文件,
			// int splitNumber = adviceNumber;
			int splitNumber = this.sourceFiles.size();
			if (0 == splitNumber) {
				//throw DataXException.asDataXException(FtpReaderErrorCode.EMPTY_DIR_EXCEPTION,
				//		String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.originConfig.getString(Key.PATH)));
				String strJson = JSONObject.toJSONString(new ArrayList<FileInfo>());
				Configuration splitedConfig = this.originConfig.clone();
				splitedConfig.set(Constant.SOURCE_FILES, strJson);
				readerSplitConfigs.add(splitedConfig);
				
			}else {
				List<List<FileInfo>> splitedSourceFiles = this.splitSourceFiles(new ArrayList(this.sourceFiles),
						splitNumber);
				for (List<FileInfo> files : splitedSourceFiles) {
					Configuration splitedConfig = this.originConfig.clone();
					// splitedConfig.set(Constant.SOURCE_FILES, files);
					String strJson = JSONObject.toJSONString(files);
					splitedConfig.set(Constant.SOURCE_FILES, strJson);
					readerSplitConfigs.add(splitedConfig);
				}
			}
			LOG.debug("split() ok and end...");
			return readerSplitConfigs;
		}

		private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
			List<List<T>> splitedList = new ArrayList<List<T>>();
			int averageLength = sourceList.size() / adviceNumber;
			averageLength = averageLength == 0 ? 1 : averageLength;

			for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
				end = begin + averageLength;
				if (end > sourceList.size()) {
					end = sourceList.size();
				}
				splitedList.add(sourceList.subList(begin, end));
			}
			return splitedList;
		}
		
		private boolean isTargetFile(String serverId,FileInfo file,String readMode) {
			// 过滤~$ 开头的excel文件
			int ch = file.getPath().lastIndexOf(File.separatorChar);
			if (ch > 0 && ch < file.getPath().length()-1) {
				String fileName = file.getPath().substring(ch + 1).toLowerCase();
				if(fileName.startsWith("~$") && (fileName.endsWith(".xls") || fileName.endsWith(".xlsx"))) {
					return false;
				}
			}
			
			return statusManage.isNewFile(serverId, file, readMode);
		}
	}

	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private String host;
		private int port;
		private String username;
		private String password;
		private String protocol;
		private int timeout;
		private String connectPattern;

		private Configuration readerSliceConfig;
		private List<FileInfo> sourceFiles;

		private FtpHelper ftpHelper = null;
		private FileStatusManage statusManage = new FileStatusManage();
		
		@Override
		public void init() {//连接重试
			/* for ftp connection */
			this.readerSliceConfig = this.getPluginJobConf();
			this.host = readerSliceConfig.getString(Key.HOST);
			this.protocol = readerSliceConfig.getString(Key.PROTOCOL);
			//优先从密码文件中读取用户名、密码
			String passwordFile = readerSliceConfig.getString(Key.PASSWORD_FILE);
			if( passwordFile != null && new File(passwordFile).exists()) {
				Properties p = new Properties();
				try {
					p.load(new FileInputStream(passwordFile));
					this.username = p.getProperty(Key.USERNAME);
					this.password = p.getProperty(Key.PASSWORD);
					
				} catch (Exception e) {
					System.out.println(e.getMessage());
					throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
							String.format("密码文件打开失败: [%s]", e.getMessage()));
				}
			}else {
				this.username = readerSliceConfig.getString(Key.USERNAME);
				this.password = readerSliceConfig.getString(Key.PASSWORD);
			}

			this.timeout = readerSliceConfig.getInt(Key.TIMEOUT, Constant.DEFAULT_TIMEOUT);

			//this.sourceFiles = this.readerSliceConfig.getList(Constant.SOURCE_FILES, FileInfo.class);
			String strJson = this.readerSliceConfig.getString(Constant.SOURCE_FILES);
			this.sourceFiles = JSONObject.parseArray(strJson,FileInfo.class);
			
			if ("sftp".equals(protocol)) {
				//sftp协议
				this.port = readerSliceConfig.getInt(Key.PORT, Constant.DEFAULT_SFTP_PORT);
				this.ftpHelper = new SftpHelper();
			} else if ("ftp".equals(protocol)) {
				// ftp 协议
				this.port = readerSliceConfig.getInt(Key.PORT, Constant.DEFAULT_FTP_PORT);
				this.connectPattern = readerSliceConfig.getString(Key.CONNECTPATTERN, Constant.DEFAULT_FTP_CONNECT_PATTERN);// 默认为被动模式
				this.ftpHelper = new StandardFtpHelper();
			}	
			boolean bBinaryFile = readerSliceConfig.getBool(Key.BINARY_FILE,false);
			ftpHelper.loginFtpServer(host, username, password, port, timeout, connectPattern, bBinaryFile);
		}

		@Override
		public void prepare() {

		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {
			try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String.format(
                        "关闭与ftp服务器连接失败: [%s] host=%s, username=%s, port=%s",
                        e.getMessage(), host, username, port);
                LOG.error(message, e);
            }
		}

		@Override
		public void startRead(RecordSender recordSender) {
			LOG.debug("start read source files...");
			
			for (FileInfo f : this.sourceFiles) {
				String fileName = f.getPath();
				LOG.info(String.format("reading file : [%s]", fileName));
				InputStream inputStream = null;
				
				try {
										
					String fileExt = fileName.substring(fileName.lastIndexOf('.'));
					FileFormat fmt;
					
					if(".xls".equalsIgnoreCase(fileExt)){
						fmt = FileFormat.EXCEL2003;
					}else if (".xlsx".equalsIgnoreCase(fileExt)) {
						fmt = FileFormat.EXCEL2007;
					}else {
						fmt = FileFormat.TEXT;
					}
					
					if (fmt == FileFormat.TEXT) {
						//使用BOMInputStream 去除UTF-8 with BOM头三个标记字节
						inputStream = new BOMInputStream(ftpHelper.getInputStream(fileName));
						UnstructuredStorageReaderUtil.readFromStream(inputStream, "[FTP]"+fileName, this.readerSliceConfig,
								recordSender, this.getTaskPluginCollector());
					}else {
						inputStream = ftpHelper.getInputStream(fileName);
						UnstructuredStorageReaderUtil.readFromExcel( inputStream,fmt, "[FTP]"+fileName,
									this.readerSliceConfig, recordSender, this.getTaskPluginCollector());
					}
				}finally {
					if (inputStream != null) {
						try {
							inputStream.close();							
						} catch (IOException e) {
						}
						ftpHelper.readFilePost();
					}
				}
				recordSender.flush();
			}
			
			//TODO: 如果脏数据超过阈值，不更新文件状态
			String readMode = this.readerSliceConfig.getString(
					com.alibaba.datax.plugin.unstructuredstorage.reader.Key.READ_MODE,
					com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.READ_FULL);
			
			if (!readMode.equals(com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.READ_FULL)) {
				String fileServerId = this.readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FILESERVER_ID,"");
				
				for (FileInfo f : sourceFiles) {
					statusManage.updateStatus(fileServerId, f);
				}
			}
			LOG.debug("end read source files...");
		}

	}
}
