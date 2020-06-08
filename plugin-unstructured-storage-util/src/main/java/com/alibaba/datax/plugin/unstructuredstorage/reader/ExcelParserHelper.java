package com.alibaba.datax.plugin.unstructuredstorage.reader;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author justin 代码基于 https://github.com/SwordfallYeung/POIExcel 修改
 */
public class ExcelParserHelper {

	/**
	 * 
	 * @param filePath      要读取的Excel文件全路径
	 * @param headerLine    表头所在的行号
	 * @param usecols       要读取的字段
	 * @param sheetsRegex   匹配要读取的Sheet页名称的正则表达式，如果sheetsIndex不为null时忽略本参数
	 * @param skipHeader    是否输出表头行
	 * @param numericFormat 数值类型的替换格式，为null时保留原数字格式
	 * @param sheetsIndex   要读取的Sheet页索引号列表，从1开始
	 * @return 返回从Excel文件中读出的记录集，读取失败返回null
	 * 
	 * @throws Exception
	 */
	public static List<String[]> parse(String filePath, int headerLine, String[] usecols, int[] sheetsIndexList,
			String sheetsRegex, boolean skipHeader, String numericFormat) throws Exception {
		
		InputStream inputStream = new FileInputStream(filePath);
		FileFormat format = (filePath.toLowerCase().endsWith(".xls")) ? FileFormat.EXCEL2003 : FileFormat.EXCEL2007;
		
		List<Integer> list = new ArrayList<Integer>();
		if(sheetsIndexList != null) {
			for(int i : sheetsIndexList) {
				list.add(i);
			}
		}
		return parse(inputStream,format,headerLine,usecols,list,sheetsRegex,skipHeader, numericFormat);
		
	}

	public static List<String[]> parse(InputStream inputStream, FileFormat format,int headerLine, String[] usecols, List sheetsIndexList,
			String sheetsRegex, boolean skipHeader, String numericFormat) throws Exception {
		
		int[] indexs = null;
		if( sheetsIndexList != null && sheetsIndexList.size() > 0) {
			indexs = new int[sheetsIndexList.size()];
			for(int i=0; i< sheetsIndexList.size(); i++) {
				try {
					indexs[i] = (int) sheetsIndexList.get(i);
				}catch(Exception e) {}
			}
		}
		if (format == FileFormat.EXCEL2007) {
			Excel2007ParserDefaultHandler parser = new Excel2007ParserDefaultHandler();
			return parser.process(inputStream, headerLine, usecols, indexs, sheetsRegex, skipHeader, numericFormat);

		} else if (format == FileFormat.EXCEL2003) {
			Excel2003Parser parser = new Excel2003Parser();
			return parser.process(inputStream, headerLine, usecols, indexs, sheetsRegex, skipHeader, numericFormat);
		}
		else {
			System.out.println("非Excel文件，无法读取");
			return null;
		}
	}
}
