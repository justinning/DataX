package com.alibaba.datax.plugin.unstructuredstorage.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.anarres.lzo.LzoDecompressor1x_safe;
import org.anarres.lzo.LzoInputStream;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.csvreader.CsvReader;

import io.airlift.compress.snappy.SnappyCodec;
import io.airlift.compress.snappy.SnappyFramedInputStream;

public class UnstructuredStorageReaderUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(UnstructuredStorageReaderUtil.class);
	public static HashMap<String, Object> csvReaderConfigMap;

	private UnstructuredStorageReaderUtil() {

	}

	/**
	 * @param inputLine
	 *            输入待分隔字符串
	 * @param delimiter
	 *            字符串分割符
	 * @return 分隔符分隔后的字符串数组，出现异常时返回为null 支持转义，即数据中可包含分隔符
	 * */
	public static String[] splitOneLine(String inputLine, char delimiter) {
		String[] splitedResult = null;
		if (null != inputLine) {
			try {
				CsvReader csvReader = new CsvReader(new StringReader(inputLine));
				csvReader.setDelimiter(delimiter);

				setCsvReaderConfig(csvReader);

				if (csvReader.readRecord()) {
					splitedResult = csvReader.getValues();
				}
			} catch (IOException e) {
				// nothing to do
			}
		}
		return splitedResult;
	}

	public static String[] splitBufferedReader(CsvReader csvReader)
			throws IOException {

		String[] splitedResult = null;
					
		if (csvReader.readRecord()) {
			splitedResult = csvReader.getValues();
		}
		return splitedResult;
	}
	
	/**
	 * 不支持转义
	 *
	 * @return 分隔符分隔后的字符串数，
	 * */
	public static String[] splitOneLine(String inputLine, String delimiter) {
		String[] splitedResult = StringUtils.split(inputLine, delimiter);
		return splitedResult;
	}

	public static void readFromStream(InputStream inputStream, String context,
									  Configuration readerSliceConfig, RecordSender recordSender,
									  TaskPluginCollector taskPluginCollector) {
		String compress = readerSliceConfig.getString(Key.COMPRESS, null);
		if (StringUtils.isBlank(compress)) {
			compress = null;
		}
		String encoding = readerSliceConfig.getString(Key.ENCODING,
				Constant.DEFAULT_ENCODING);
		// handle blank encoding
		if (StringUtils.isBlank(encoding)) {
			encoding = Constant.DEFAULT_ENCODING;
			LOG.warn(String.format("您配置的encoding为[%s], 使用默认值[%s]", encoding,
					Constant.DEFAULT_ENCODING));
		}

		List<Configuration> column = readerSliceConfig
				.getListConfiguration(Key.COLUMN);
		// handle ["*"] -> [], null
		if (null != column && 1 == column.size()
				&& "\"*\"".equals(column.get(0).toString())) {
			readerSliceConfig.set(Key.COLUMN, null);
			column = null;
		}

		BufferedReader reader = null;
		int bufferSize = readerSliceConfig.getInt(Key.BUFFER_SIZE,
				Constant.DEFAULT_BUFFER_SIZE);

		// compress logic
		try {
			if (null == compress) {
				reader = new BufferedReader(new InputStreamReader(inputStream,
						encoding), bufferSize);
			} else {
				// TODO compress
				if ("lzo_deflate".equalsIgnoreCase(compress)) {
					LzoInputStream lzoInputStream = new LzoInputStream(
							inputStream, new LzoDecompressor1x_safe());
					reader = new BufferedReader(new InputStreamReader(
							lzoInputStream, encoding));
				} else if ("lzo".equalsIgnoreCase(compress)) {
					LzoInputStream lzopInputStream = new ExpandLzopInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							lzopInputStream, encoding));
				} else if ("gzip".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new GzipCompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding), bufferSize);
				} else if ("bzip2".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new BZip2CompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding), bufferSize);
				} else if ("hadoop-snappy".equalsIgnoreCase(compress)) {
					CompressionCodec snappyCodec = new SnappyCodec();
					InputStream snappyInputStream = snappyCodec.createInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							snappyInputStream, encoding));
				} else if ("framing-snappy".equalsIgnoreCase(compress)) {
					InputStream snappyInputStream = new SnappyFramedInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							snappyInputStream, encoding));
				}/* else if ("lzma".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new LZMACompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding));
				} *//*else if ("pack200".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new Pack200CompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding));
				} *//*else if ("xz".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new XZCompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, encoding));
				} else if ("ar".equalsIgnoreCase(compress)) {
					ArArchiveInputStream arArchiveInputStream = new ArArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							arArchiveInputStream, encoding));
				} else if ("arj".equalsIgnoreCase(compress)) {
					ArjArchiveInputStream arjArchiveInputStream = new ArjArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							arjArchiveInputStream, encoding));
				} else if ("cpio".equalsIgnoreCase(compress)) {
					CpioArchiveInputStream cpioArchiveInputStream = new CpioArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							cpioArchiveInputStream, encoding));
				} else if ("dump".equalsIgnoreCase(compress)) {
					DumpArchiveInputStream dumpArchiveInputStream = new DumpArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							dumpArchiveInputStream, encoding));
				} else if ("jar".equalsIgnoreCase(compress)) {
					JarArchiveInputStream jarArchiveInputStream = new JarArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							jarArchiveInputStream, encoding));
				} else if ("tar".equalsIgnoreCase(compress)) {
					TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							tarArchiveInputStream, encoding));
				}*/
				else if ("zip".equalsIgnoreCase(compress)) {
					ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							zipCycleInputStream, encoding), bufferSize);
				} else {
					throw DataXException
							.asDataXException(
									UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
									String.format("仅支持 gzip, bzip2, zip, lzo, lzo_deflate, hadoop-snappy, framing-snappy" +
											"文件压缩格式 , 不支持您配置的文件压缩格式: [%s]", compress));
				}
			}
			
			
			UnstructuredStorageReaderUtil.doReadFromStream(reader, context,
					readerSliceConfig, recordSender, taskPluginCollector);
		} catch (UnsupportedEncodingException uee) {
			throw DataXException
					.asDataXException(
							UnstructuredStorageReaderErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
							String.format("不支持的编码格式 : [%s]", encoding), uee);
		} catch (NullPointerException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.RUNTIME_EXCEPTION,
					"运行时错误, 请联系我们", e);
		}/* catch (ArchiveException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
					String.format("压缩文件流读取错误 : [%s]", context), e);
		} */catch (IOException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
					String.format("流读取错误 : [%s]", context), e);
		} finally {
			IOUtils.closeQuietly(reader);
		}

	}

	public static void doReadFromStream(BufferedReader reader, String context,
										Configuration readerSliceConfig, RecordSender recordSender,
										TaskPluginCollector taskPluginCollector) {
		String encoding = readerSliceConfig.getString(Key.ENCODING,
				Constant.DEFAULT_ENCODING);
		Character fieldDelimiter = null;
		String delimiterInStr = readerSliceConfig
				.getString(Key.FIELD_DELIMITER);
		if (null != delimiterInStr && 1 != delimiterInStr.length()) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
					String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
		}
		if (null == delimiterInStr) {
			LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
					Constant.DEFAULT_FIELD_DELIMITER));
		}

		// warn: default value ',', fieldDelimiter could be \n(lineDelimiter)
		// for no fieldDelimiter
		fieldDelimiter = readerSliceConfig.getChar(Key.FIELD_DELIMITER,
				Constant.DEFAULT_FIELD_DELIMITER);
		Boolean skipHeader = readerSliceConfig.getBool(Key.SKIP_HEADER,
				Constant.DEFAULT_SKIP_HEADER);
		// warn: no default value '\N'
		String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);
		
		boolean fileAttrs = readerSliceConfig.getBool(Key.READ_FILE_ATTRS,false);
		// Identify whether the first line is found.
		
		// warn: Configuration -> List<ColumnEntry> for performance
		// List<Configuration> column = readerSliceConfig
		// .getListConfiguration(Key.COLUMN);
		
		List<ColumnEntry> column = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);
				
		String[] usecols = null;
		
		//分析是否指定了表头。如果使用getListConfiguration，每个Item包含双层引号,getList返回的是JSONArray对象，单层引号
		List<Object> cl = readerSliceConfig.getList(Key.COLUMN);
		if (cl != null && cl.size() > 0) {
			String columnsInStr = cl.get(0).toString();
			if (!columnsInStr.matches(".*\\{.*\\}.*")) {
				// 从配置中获得要读取的字段列表
				usecols = new String[cl.size()];
				for (int i = 0; i < cl.size(); i++)
					usecols[i] = cl.get(i).toString();
			}
		}
		
		
		String splitLine = readerSliceConfig.getString(Key.FILE_SPLIT_LINE,"");
		boolean isBeforeSplitLine = readerSliceConfig.getBool(Key.BEFORE_SPLIT_LINE,true);
		boolean bFindFirstLine = isBeforeSplitLine;
		CsvReader csvReader  = null;
		
		// every line logic
		try {
			// TODO lineDelimiter

			csvReader = new CsvReader(reader);
			csvReader.setDelimiter(fieldDelimiter);
			
			// Read the csvReaderConfig configuration in JSON. The previous code is not found to call it. Why?
			validateCsvReaderConfig(readerSliceConfig);			
			setCsvReaderConfig(csvReader);
			
			
			//补充2列文件属性：文件路径、文件行号
			if(fileAttrs && column != null && column.size() > 0) {
				for(int i = 2; i > 0; i--) {
					ColumnEntry c = new ColumnEntry();
					c.setType("string");
					c.setIndex(column.size());
					column.add(c);
				}
			}
			
			int fileLineNum = 0;
			int recordRow = 0;
			String[] parseRow = null;
			
			while (csvReader.readRecord()) {				
				// 如果启用了comment，fileLineNum得到的行号可能不是文件的实际行号
				fileLineNum++;

				String rawData = csvReader.getRawRecord();			
				// 如果找到分隔行，如果取分割行之前的数据，则终止循环，否则以该行的字段值为新的表头，且下次不再比较splitLine
				if( !"".equals(splitLine) && (!bFindFirstLine || isBeforeSplitLine ) && rawData != null && rawData.startsWith(splitLine)){
					if(!bFindFirstLine){
						bFindFirstLine = true;
					}else {
						// 已到分割行，代表记录已结束
						break;
					}
				}
				
				if(!bFindFirstLine)
					continue;
								
				recordRow++;
				
				if( recordRow == 1 ) {
					parseRow = csvReader.getValues();
					// csvReader会重设indexByName，后续就可以调用get(name)得到字段值
					csvReader.setHeaders(parseRow);
					
					if (skipHeader) {
						LOG.info(String.format("Header line %s has been skiped.",rawData));
						continue;
					}
				}
				
				if( usecols == null) {
					parseRow = csvReader.getValues();	
				}else {
					parseRow = new String[usecols.length];
					for(int i=0; i< usecols.length; i++) {
						int index = csvReader.getIndex(usecols[i]);
						if( index >= 0 ) {
							parseRow[i] = csvReader.get(index);
						}else {
							parseRow[i] = null;
						}
					}
				}
				
				String[] newRows;
				
				// Supplementary file attributes: file path, file last modification time, and file line number where the record is located.
				// Note: If it is a compressed file, the file line number of multiple records in the same file will be the same
				if (fileAttrs) {
					newRows = new String[ parseRow.length + 2];
					System.arraycopy(parseRow, 0, newRows, 0, parseRow.length);
					
					if (recordRow == 1 && !skipHeader) {						
						newRows[parseRow.length] = Constant.FIELD_NAME_FILEPATH;
						newRows[parseRow.length + 1 ] = Constant.FIELD_NAME_LINE;
						
					}else {
						newRows[parseRow.length] = context;  //filepath
						newRows[parseRow.length + 1 ] = String.valueOf(fileLineNum);

					}
				}else {
							
					newRows = parseRow;
				}
				
				UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
						column, newRows, nullFormat, taskPluginCollector);
				
			}
				
		} catch (UnsupportedEncodingException uee) {
			throw DataXException
					.asDataXException(
							UnstructuredStorageReaderErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
							String.format("不支持的编码格式 : [%s]", encoding), uee);
		} catch (FileNotFoundException fnfe) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.FILE_NOT_EXISTS,
					String.format("无法找到文件 : [%s]", context), fnfe);
		} catch (IOException ioe) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
					String.format("读取文件错误 : [%s]", context), ioe);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.RUNTIME_EXCEPTION,
					String.format("运行时异常 : %s", e.getMessage()), e);
		} finally {
			csvReader.close();
			IOUtils.closeQuietly(reader);
		}
	}


	public static void readFromExcel(InputStream inputStream, FileFormat format, Configuration readerSliceConfig,
			String context, RecordSender recordSender, TaskPluginCollector taskPluginCollector) {

		// warn: no default value '\N'
		String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);
		String numericFormat = readerSliceConfig.getString(Key.NUMERIC_FORMAT);
		
		// column null,["*"]及["columnName",...] 三种情况均返回null，配置为JSON格式时返回非null
		List<ColumnEntry> column = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig,Key.COLUMN);
		
		boolean fileAttrs = readerSliceConfig.getBool(Key.READ_FILE_ATTRS, false);
		boolean skipHeader = readerSliceConfig.getBool(Key.SKIP_HEADER, false);
		
		try {
			String[] usecols = null;
			
			//分析是否指定了表头。如果使用getListConfiguration，每个Item包含双层引号,getList返回的是JSONArray对象，单层引号
			List<Object> cl = readerSliceConfig.getList(Key.COLUMN);
			if (cl != null && cl.size() > 0) {
				String columnsInStr = cl.get(0).toString();
				if (!columnsInStr.matches(".*\\{.*\\}.*")) {
					// 从配置中获得要读取的字段列表
					usecols = new String[cl.size()];
					for (int i = 0; i < cl.size(); i++)
						usecols[i] = cl.get(i).toString();
				}
			}
			
			//补充2列文件属性：文件路径、文件行号
			if(fileAttrs && column != null && column.size() > 0) {
				for(int i = 2; i > 0; i--) {
					ColumnEntry c = new ColumnEntry();
					c.setType("string");
					c.setIndex(column.size());
					column.add(c);
				}
			}
			int headerLine = readerSliceConfig.getInt(Key.HEADERLINE,1);
			
			//大文件需要较大内存, 200m文件 大约-Xms4096m -Xmx4096m 
			List<String[]> dataFrame = ExcelParser.parse(inputStream,
					format,
					headerLine,
					usecols,
					readerSliceConfig.getList(Key.SHEET_INDEXS),
					readerSliceConfig.getString(Key.SHEET_NAMES,null),
					skipHeader, numericFormat);
			
			int rowNum = 0;
			
			for (String[] parseRows : dataFrame) {
				rowNum++;
								
				String[] newRows;
				
				// Supplementary file attributes: file path, file last modification time, and file line number where the record is located.
				// Note: If it is a compressed file, the file line number of multiple records in the same file will be the same
				if (fileAttrs) {
					newRows = new String[ parseRows.length + 2];
					System.arraycopy(parseRows, 0, newRows, 0, parseRows.length);
					
					if (rowNum == 1 && !skipHeader) {
						newRows[parseRows.length] = Constant.FIELD_NAME_FILEPATH;
						newRows[parseRows.length + 1 ] = Constant.FIELD_NAME_LINE;
						
					}else {
						newRows[parseRows.length] = context;  //filepath
						newRows[parseRows.length + 1 ] = String.valueOf(headerLine + rowNum - (skipHeader ? 0:1) );
					}
				}else {
									
					newRows = parseRows;
				}
				
				// every line logic
				UnstructuredStorageReaderUtil.transportOneRecord(recordSender, column, newRows, nullFormat,
						taskPluginCollector);
			}
		} catch (FileNotFoundException fnfe) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.FILE_NOT_EXISTS,
					String.format("无法找到文件 : [%s]", inputStream), fnfe);
		} catch (IOException ioe) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
					String.format("读取文件错误 : [%s]", inputStream), ioe);
		} catch (Exception e) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.RUNTIME_EXCEPTION,
					String.format("运行时异常 : %s", e.getMessage()), e);
		}

	}
	
	public static Record transportOneRecord(RecordSender recordSender,
											Configuration configuration,
											TaskPluginCollector taskPluginCollector,
											String line){
		List<ColumnEntry> column = UnstructuredStorageReaderUtil
				.getListColumnEntry(configuration, Key.COLUMN);
		// 注意: nullFormat 没有默认值
		String nullFormat = configuration.getString(Key.NULL_FORMAT);
		String delimiterInStr = configuration.getString(Key.FIELD_DELIMITER);
		if (null != delimiterInStr && 1 != delimiterInStr.length()) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
					String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
		}
		if (null == delimiterInStr) {
			LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
					Constant.DEFAULT_FIELD_DELIMITER));
		}
		// warn: default value ',', fieldDelimiter could be \n(lineDelimiter)
		// for no fieldDelimiter
		Character fieldDelimiter = configuration.getChar(Key.FIELD_DELIMITER,
				Constant.DEFAULT_FIELD_DELIMITER);

		String[] sourceLine = StringUtils.split(line, fieldDelimiter);

		return transportOneRecord(recordSender, column, sourceLine, nullFormat, taskPluginCollector);
	}

	public static Record transportOneRecord(RecordSender recordSender,
											List<ColumnEntry> columnConfigs, String[] sourceLine,
											String nullFormat, TaskPluginCollector taskPluginCollector) {
		Record record = recordSender.createRecord();
		Column columnGenerated = null;

		// 创建都为String类型column的record
		if (null == columnConfigs || columnConfigs.size() == 0) {
			for (String columnValue : sourceLine) {
				// not equalsIgnoreCase, it's all ok if nullFormat is null
				if (columnValue == null || columnValue.equals(nullFormat)) {
					columnGenerated = new StringColumn(null);
				} else {
					columnGenerated = new StringColumn(columnValue);
				}
				record.addColumn(columnGenerated);
			}
			recordSender.sendToWriter(record);
		} else {
			try {
				for (ColumnEntry columnConfig : columnConfigs) {
					String columnType = columnConfig.getType();
					Integer columnIndex = columnConfig.getIndex();
					String columnConst = columnConfig.getValue();

					String columnValue = null;

					if (null == columnIndex && null == columnConst) {
						throw DataXException
								.asDataXException(
										UnstructuredStorageReaderErrorCode.NO_INDEX_VALUE,
										"由于您配置了type, 则至少需要配置 index 或 value");
					}

					if (null != columnIndex && null != columnConst) {
						throw DataXException
								.asDataXException(
										UnstructuredStorageReaderErrorCode.MIXED_INDEX_VALUE,
										"您混合配置了index, value, 每一列同时仅能选择其中一种");
					}

					if (null != columnIndex) {
						if (columnIndex >= sourceLine.length) {
							String message = String
									.format("您尝试读取的列越界,源文件该行有 [%s] 列,您尝试读取第 [%s] 列, 数据详情[%s]",
											sourceLine.length, columnIndex + 1,
											StringUtils.join(sourceLine, ","));
							LOG.warn(message);
							throw new IndexOutOfBoundsException(message);
						}

						columnValue = sourceLine[columnIndex];
					} else {
						columnValue = columnConst;
					}
					Type type = Type.valueOf(columnType.toUpperCase());
					// it's all ok if nullFormat is null
					if (columnValue.equals(nullFormat)) {
						columnValue = null;
					}
					switch (type) {
						case STRING:
							columnGenerated = new StringColumn(columnValue);
							break;
						case LONG:
							try {
								columnGenerated = new LongColumn(columnValue);
							} catch (Exception e) {
								throw new IllegalArgumentException(String.format(
										"类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
										"LONG"));
							}
							break;
						case DOUBLE:
							try {
								columnGenerated = new DoubleColumn(columnValue);
							} catch (Exception e) {
								throw new IllegalArgumentException(String.format(
										"类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
										"DOUBLE"));
							}
							break;
						case BOOLEAN:
							try {
								columnGenerated = new BoolColumn(columnValue);
							} catch (Exception e) {
								throw new IllegalArgumentException(String.format(
										"类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
										"BOOLEAN"));
							}

							break;
						case DATE:
							try {
								if (columnValue == null) {
									Date date = null;
									columnGenerated = new DateColumn(date);
								} else {
									String formatString = columnConfig.getFormat();
									//if (null != formatString) {
									if (StringUtils.isNotBlank(formatString)) {
										// 用户自己配置的格式转换, 脏数据行为出现变化
										DateFormat format = columnConfig
												.getDateFormat();
										columnGenerated = new DateColumn(
												format.parse(columnValue));
									} else {
										// 框架尝试转换
										columnGenerated = new DateColumn(
												new StringColumn(columnValue)
														.asDate());
									}
								}
							} catch (Exception e) {
								throw new IllegalArgumentException(String.format(
										"类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
										"DATE"));
							}
							break;
						default:
							String errorMessage = String.format(
									"您配置的列类型暂不支持 : [%s]", columnType);
							LOG.error(errorMessage);
							throw DataXException
									.asDataXException(
											UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
											errorMessage);
					}

					record.addColumn(columnGenerated);

				}
				recordSender.sendToWriter(record);
			} catch (IllegalArgumentException iae) {
				taskPluginCollector
						.collectDirtyRecord(record, iae.getMessage());
			} catch (IndexOutOfBoundsException ioe) {
				taskPluginCollector
						.collectDirtyRecord(record, ioe.getMessage());
			} catch (Exception e) {
				if (e instanceof DataXException) {
					throw (DataXException) e;
				}
				// 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
				taskPluginCollector.collectDirtyRecord(record, e.getMessage());
			}
		}

		return record;
	}


	public static List<ColumnEntry> getListColumnEntry(
			Configuration configuration, final String path) {
		List<JSONObject> lists = configuration.getList(path, JSONObject.class);
		if (lists == null) {
			return null;
		}
		
		try {
			List<ColumnEntry> result = new ArrayList<ColumnEntry>();
			for (final JSONObject object : lists) {
				result.add(JSON.parseObject(object.toJSONString(),
						ColumnEntry.class));
			}
			return result;
		}
		catch (Exception e) {
			return null;
		}
	}
	
	
	private enum Type {
		STRING, LONG, BOOLEAN, DOUBLE, DATE, ;
	}

	/**
	 * check parameter:encoding, compress, filedDelimiter
	 * */
	public static void validateParameter(Configuration readerConfiguration) {

		// encoding check
		validateEncoding(readerConfiguration);

		//only support compress types
		validateCompress(readerConfiguration);

		//fieldDelimiter check
		validateFieldDelimiter(readerConfiguration);

		// column: 1. index type 2.value type 3.when type is Date, may have format
		validateColumn(readerConfiguration);
		
		// fileSplitLine
		String splitLine = readerConfiguration.getString(Key.FILE_SPLIT_LINE);			
		if( "".equals(splitLine)) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,String.format("文件分隔行(%s)不能配置为空",
							Key.FILE_SPLIT_LINE));
		}
		Boolean beforeSplitLine = readerConfiguration.getBool(Key.BEFORE_SPLIT_LINE);
		if ( beforeSplitLine != null && splitLine == null) {
			throw DataXException.asDataXException(
					UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,String.format("配置了%s，但未配置文件分隔行%s",
							Key.BEFORE_SPLIT_LINE,
							Key.FILE_SPLIT_LINE));
		}

	}

	public static void validateEncoding(Configuration readerConfiguration) {
		// encoding check
		String encoding = readerConfiguration
				.getString(
						com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
						com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
		try {
			encoding = encoding.trim();
			readerConfiguration.set(Key.ENCODING, encoding);
			Charsets.toCharset(encoding);
		} catch (UnsupportedCharsetException uce) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
					String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
		} catch (Exception e) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.CONFIG_INVALID_EXCEPTION,
					String.format("编码配置异常, 请联系我们: %s", e.getMessage()), e);
		}
	}

	public static void validateCompress(Configuration readerConfiguration) {
		String compress =readerConfiguration
				.getUnnecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS,null,null);
		if(StringUtils.isNotBlank(compress)){
			compress = compress.toLowerCase().trim();
			boolean compressTag = "gzip".equals(compress) || "bzip2".equals(compress) || "zip".equals(compress)
					|| "lzo".equals(compress) || "lzo_deflate".equals(compress) || "hadoop-snappy".equals(compress)
					|| "framing-snappy".equals(compress);
			if (!compressTag) {
				throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
						String.format("仅支持 gzip, bzip2, zip, lzo, lzo_deflate, hadoop-snappy, framing-snappy " +
								"文件压缩格式, 不支持您配置的文件压缩格式: [%s]", compress));
			}
		}else{
			// 用户可能配置的是 compress:"",空字符串,需要将compress设置为null
			compress = null;
		}
		readerConfiguration.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS, compress);

	}

	public static void validateFieldDelimiter(Configuration readerConfiguration) {
		//fieldDelimiter check
		String delimiterInStr = readerConfiguration.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER,
				String.valueOf(Constant.DEFAULT_FIELD_DELIMITER));
		if(null == delimiterInStr){
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.REQUIRED_VALUE,
					String.format("您提供配置文件有误，[%s]是必填参数.",
							com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER));
		}else if(1 != delimiterInStr.length()){
			// warn: if have, length must be one
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
					String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
		}
	}

	public static void validateColumn(Configuration readerConfiguration) {
		
		// column: 1. index type 2.value type 3.when type is Date, may have
		// format
		List<Configuration> columns = readerConfiguration
				.getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
		if (null == columns || columns.size() == 0) {
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.REQUIRED_VALUE, "您需要指定 columns");
		}
		// handle ["*"]
		boolean jsonFormat = false;
		if (null != columns && columns.size() > 0 ) {
			
			String columnsInStr = columns.get(0).toString();
			if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
				readerConfiguration.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN, null);				
			}else if( columnsInStr.matches(".*\\{.*\\}.*")) {
				jsonFormat = true;
			}else {
				//指定列名方式
			}
		}

		if (jsonFormat && null != columns && columns.size() != 0) {
			for (Configuration eachColumnConf : columns) {
				eachColumnConf.getNecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE,
						UnstructuredStorageReaderErrorCode.REQUIRED_VALUE);
				Integer columnIndex = eachColumnConf
						.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
				String columnValue = eachColumnConf
						.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

				if (null == columnIndex && null == columnValue) {
					throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.NO_INDEX_VALUE,
							"由于您配置了type, 则至少需要配置 index 或 value");
				}

				if (null != columnIndex && null != columnValue) {
					throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.MIXED_INDEX_VALUE,
							"您混合配置了index, value, 每一列同时仅能选择其中一种");
				}
				if (null != columnIndex && columnIndex < 0) {
					throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
							String.format("index需要大于等于0, 您配置的index为[%s]", columnIndex));
				}
			}
		}
	}

	public static void validateCsvReaderConfig(Configuration readerConfiguration) {
		String  csvReaderConfig = readerConfiguration.getString(Key.CSV_READER_CONFIG);
		if(StringUtils.isNotBlank(csvReaderConfig)){
			try{
				UnstructuredStorageReaderUtil.csvReaderConfigMap = JSON.parseObject(csvReaderConfig, new TypeReference<HashMap<String, Object>>() {});
			}catch (Exception e) {
				LOG.info(String.format("WARN!!!!忽略csvReaderConfig配置! 配置错误,值只能为空或者为Map结构,您配置的值为: %s", csvReaderConfig));
			}
		}
	}

	/**
	 *
	 * @Title: getRegexPathParent
	 * @Description: 获取正则表达式目录的父目录
	 * @param @param regexPath
	 * @param @return
	 * @return String
	 * @throws
	 */
	public static String getRegexPathParent(String regexPath){
		int endMark;
		for (endMark = 0; endMark < regexPath.length(); endMark++) {
			if ('*' != regexPath.charAt(endMark) && '?' != regexPath.charAt(endMark)) {
				continue;
			} else {
				break;
			}
		}
		int lastDirSeparator = regexPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
		String parentPath  = regexPath.substring(0,lastDirSeparator + 1);

		return  parentPath;
	}
	/**
	 *
	 * @Title: getRegexPathParentPath
	 * @Description: 获取含有通配符路径的父目录，目前只支持在最后一级目录使用通配符*或者?.
	 * (API jcraft.jsch.ChannelSftp.ls(String path)函数限制)  http://epaul.github.io/jsch-documentation/javadoc/
	 * @param @param regexPath
	 * @param @return
	 * @return String
	 * @throws
	 */
	public static String getRegexPathParentPath(String regexPath){
		int lastDirSeparator = regexPath.lastIndexOf(IOUtils.DIR_SEPARATOR);
		String parentPath = "";
		parentPath = regexPath.substring(0,lastDirSeparator + 1);
		if(parentPath.contains("*") || parentPath.contains("?")){
			throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
					String.format("配置项目path中：[%s]不合法，目前只支持在最后一级目录使用通配符*或者?", regexPath));
		}
		return parentPath;
	}

	public static void setCsvReaderConfig(CsvReader csvReader){
		if(null != UnstructuredStorageReaderUtil.csvReaderConfigMap && !UnstructuredStorageReaderUtil.csvReaderConfigMap.isEmpty()){
			try {
				BeanUtils.populate(csvReader,UnstructuredStorageReaderUtil.csvReaderConfigMap);
				LOG.info(String.format("csvReaderConfig设置成功,设置后CsvReader:%s", JSON.toJSONString(csvReader)));
			} catch (Exception e) {
				LOG.info(String.format("WARN!!!!忽略csvReaderConfig配置!通过BeanUtils.populate配置您的csvReaderConfig发生异常,您配置的值为: %s;请检查您的配置!CsvReader使用默认值[%s]",
						JSON.toJSONString(UnstructuredStorageReaderUtil.csvReaderConfigMap),JSON.toJSONString(csvReader)));
			}
		}else {
			//默认关闭安全模式, 放开10W字节的限制
			csvReader.setSafetySwitch(false);
			LOG.info(String.format("CsvReader使用默认值[%s],csvReaderConfig值为[%s]",JSON.toJSONString(csvReader),JSON.toJSONString(UnstructuredStorageReaderUtil.csvReaderConfigMap)));
		}
	}
}
