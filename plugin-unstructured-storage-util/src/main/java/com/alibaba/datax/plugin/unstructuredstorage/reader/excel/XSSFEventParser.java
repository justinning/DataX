package com.alibaba.datax.plugin.unstructuredstorage.reader.excel;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * @desc 代码基于 https://github.com/SwordfallYeung/POIExcel 修改
 *       采用SAX事件驱动模式解决XLSX文件，可以有效解决用户模式内存溢出的问题，它会自动忽略空行以节省内存
 * 
 *       增加： 1、错误公式按空值处理 2、可按传入的格式重设数值型单元格样式(如果单元格是文本型，则不起作用)
 *       3、完善日期格式化，包含文本格式的日期内容及日期格式的字符串
 **/
public class XSSFEventParser extends DefaultHandler {

	/**
	 * 单元格中的数据可能的数据类型
	 */
	enum CellDataType {
	BOOL, ERROR, FORMULA, INLINESTR, SSTINDEX, NUMBER, DATE, NULL
	}

	/**
	 * 共享字符串表
	 */
	private SharedStringsTable sst;

	/**
	 * 上一次的索引值
	 */
	private String lastIndex;

	/**
	 * 工作表索引
	 */
	private int sheetIndex = 0;

	/**
	 * sheet名
	 */
	private String sheetName = "";

	/**
	 * 总行数
	 */
	private int totalRows = 0;

	private List<String[]> dataFrame = new ArrayList<>();

	/**
	 * 一行内cell集合
	 */
	private List<String> cellList = new RowCells<>();

	/**
	 * 判断整行是否为空行的标记
	 */
	private boolean notEmptyLine = false;

	/**
	 * 当前行
	 */

	private int headerLine = 0;
	private int totalColumns = 0;
	/**
	 * 当前列
	 */
	private int curCol = 0;

	/**
	 * T元素标识
	 */
	private boolean isTElement;

	/**
	 * 判断上一单元格是否为文本空单元格
	 */
	private boolean startElementFlag = true;
	private boolean endElementFlag = false;
	private boolean charactersFlag = false;

	/**
	 * 异常信息，如果为空则表示没有异常
	 */
	private String exceptionMessage;

	/**
	 * 单元格数据类型，默认为字符串类型
	 */
	private CellDataType nextDataType = CellDataType.SSTINDEX;

	private final DataFormatter formatter = new DataFormatter();

	/**
	 * 单元格日期格式的索引
	 */
	private short formatIndex;

	/**
	 * 日期格式字符串
	 */
	private String formatString;

	// 定义前一个元素和当前元素的位置，用来计算其中空的单元格数量，如A6和A8等
	private String prePreRef = "A", preRef = null, ref = null;

	// 定义该文档一行最大的单元格数，用来补全一行最后可能缺失的单元格
	private String maxRef = null;

	private int[] readSheetsIndex = null;
	private String[] usecols = null;
	private int[] headerIndexs = null;
	private String readSheetsRegex = null;
	private boolean includeHeader = false;
	private String numericFormat = null;
	private String dateFormat = null;
	/**
	 * 单元格
	 */
	private StylesTable stylesTable;
	private static final Logger LOG = LoggerFactory.getLogger(XSSFEventParser.class);

	/**
	 * 遍历工作簿中所有的电子表格 并缓存在mySheetList中
	 * 
	 * @param inputStream   TODO
	 * @param usecols       TODO
	 * @param numericFormat TODO
	 * @param dateFormat TODO
	 * @throws Exception
	 */
	public List<String[]> process(InputStream inputStream, int headerLine, String[] usecols, int[] sheetsIndex,
			String sheetsRegex, boolean skipHeader, String numericFormat, String dateFormat) throws Exception {

		readSheetsIndex = sheetsIndex;
		readSheetsRegex = sheetsRegex;

		this.headerLine = headerLine;
		this.usecols = usecols;
		this.includeHeader = !skipHeader;
		this.numericFormat = numericFormat;
		this.dateFormat = dateFormat;
		OPCPackage pkg = OPCPackage.open(inputStream);
		XSSFReader xssfReader = new XSSFReader(pkg);
		stylesTable = xssfReader.getStylesTable();
		SharedStringsTable sst = xssfReader.getSharedStringsTable();
		XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
		this.sst = sst;
		parser.setContentHandler(this);
		XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) xssfReader.getSheetsData();
		while (sheets.hasNext()) { // 遍历sheet

			InputStream sheet = sheets.next(); // sheets.next()和sheets.getSheetName()不能换位置，否则sheetName报错
			sheetName = sheets.getSheetName();

			if (isReadSheet(sheetIndex + 1, sheetName)) {
				InputSource sheetSource = new InputSource(sheet);
				parser.parse(sheetSource); // 解析excel的每条记录，在这个过程中startElement()、characters()、endElement()这三个函数会依次执行
			}
			sheet.close();
			sheetIndex++;
			this.totalColumns = 0;
		}
		return dataFrame;
	}

	/**
	 * 第一个执行
	 *
	 * @param uri
	 * @param localName
	 * @param name
	 * @param attributes
	 * @throws SAXException
	 */
	@Override
	public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
		// c => 单元格
		if ("c".equals(name)) {

			// 前一个单元格的位置
			if (preRef == null) {
				preRef = attributes.getValue("r");

			} else {
				// 中部文本空单元格标识 ‘endElementFlag’
				// 判断前一次是否为文本空字符串，true则表明不是文本空字符串，false表明是文本空字符串跳过把空字符串的位置赋予preRef
				if (endElementFlag) {
					preRef = ref;
				}
			}

			// 当前单元格的位置
			ref = attributes.getValue("r");
			// 首部文本空单元格标识 ‘startElementFlag’
			// 判断前一次，即首部是否为文本空字符串，true则表明不是文本空字符串，false表明是文本空字符串,
			// 且已知当前格，即第二格带“B”标志，则ref赋予preRef
			if (!startElementFlag && !notEmptyLine) { // 上一个单元格为文本空单元格，执行下面的，使ref=preRef；flag为true表明该单元格之前有数据值，即该单元格不是首部空单元格，则跳过
				// 这里只有上一个单元格为文本空单元格，且之前的几个单元格都没有值才会执行
				preRef = ref;
			}

			// 设定单元格类型
			this.setNextDataType(attributes);
			endElementFlag = false;
			charactersFlag = false;
			startElementFlag = false;
		}

		// 当元素为t时
		if ("t".equals(name)) {
			isTElement = true;
		} else {
			isTElement = false;
		}

		// 置空
		lastIndex = "";
	}

	/**
	 * 第二个执行 得到单元格对应的索引值或是内容值 如果单元格类型是字符串、INLINESTR、数字、日期，lastIndex则是索引值
	 * 如果单元格类型是布尔值、错误、公式，lastIndex则是内容值
	 * 
	 * @param ch
	 * @param start
	 * @param length
	 * @throws SAXException
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		startElementFlag = true;
		charactersFlag = true;
		lastIndex += new String(ch, start, length);
	}

	/**
	 * 第三个执行
	 *
	 * @param uri
	 * @param localName
	 * @param name
	 * @throws SAXException
	 */
	@Override
	public void endElement(String uri, String localName, String name) throws SAXException {
		// t元素也包含字符串
		if (isTElement) {// 这个程序没经过
			// 将单元格内容加入rowlist中，在这之前先去掉字符串前后的空白符
			String value = lastIndex.trim();
			cellList.add(curCol, value);
			endElementFlag = true;
			curCol++;
			isTElement = false;
			// 如果里面某个单元格含有值，则标识该行不为空行
			if (value != null && !"".equals(value)) {
				notEmptyLine = true;
			}
		} else if ("v".equals(name)) {
			// v => 单元格的值，如果单元格是字符串，则v标签的值为该字符串在SST中的索引
			String value = this.getDataValue(lastIndex.trim(), "");// 根据索引值获取对应的单元格值

			// 补全单元格之间的空单元格
			if (!ref.equals(preRef)) {
				int len = countNullCell(ref, preRef);
				for (int i = 0; i < len; i++) {
					cellList.add(curCol, "");
					curCol++;
				}
			} else if (ref.equals(preRef) && !ref.startsWith("A")) { // ref等于preRef，且以B或者C...开头，表明首部为空格
				int len = countNullCell(ref, "A");
				for (int i = 0; i <= len; i++) {
					cellList.add(curCol, "");
					curCol++;
				}
			}
			cellList.add(curCol, value);
			curCol++;
			endElementFlag = true;
			// 如果里面某个单元格含有值，则标识该行不为空行
			if (value != null && !"".equals(value)) {
				notEmptyLine = true;
			}
		} else {
			// 如果标签名称为row，这说明已到行尾，调用optRows()方法
			if ("row".equals(name)) {
				String r = "-1";

				// ref不为null时为本行最后一个非空单元格的坐标，如果B1
				if (ref != null) {
					r = ref.replaceAll("[A-Z]+", "");
				} else {
					// 本行是空行
					notEmptyLine = false;
				}
				int curRow = Integer.parseInt(r);

				// 如果表头行是空行，totalColumns将始终是0，所有的数据会被忽略
				if (curRow == headerLine) {
					totalColumns = cellList.size(); // 获取列数
					// 获得headers各字段对应的列索引
					if (usecols != null && usecols.length > 0 && headerIndexs == null) {

						headerIndexs = new int[usecols.length];
						for (int i = 0; i < usecols.length; i++) {
							headerIndexs[i] = cellList.indexOf(usecols[i]);
						}
					}
				}

				if (notEmptyLine && totalColumns > 0) { // 该行不为空行

					if ((includeHeader && curRow >= headerLine) || (!includeHeader && curRow > headerLine)) {
						if (cellList.size() <= totalColumns) { // 其他行如果尾部单元格总数小于totalColums，则补全单元格
							for (int i = cellList.size(); i < totalColumns; i++) {
								cellList.add(i, "");
							}
						}

						String[] recordRow = null;
						if (headerIndexs != null) {
							recordRow = new String[headerIndexs.length];
							for (int i = 0; i < headerIndexs.length; i++)
								try {
									recordRow[i] = cellList.get(headerIndexs[i]);
								} catch (IndexOutOfBoundsException e) {
								}
						} else {
							recordRow = new String[cellList.size()];
							for (int i = 0; i < cellList.size(); i++)
								recordRow[i] = cellList.get(i);
						}

						// 引用而不是复制
						dataFrame.add(recordRow);
						totalRows++;
					}
				}

				cellList.clear();
				curCol = 0;
				preRef = null;
				prePreRef = null;
				ref = null;
				notEmptyLine = false;
			}
		}
	}

	/**
	 * 处理数据类型
	 *
	 * @param attributes
	 */
	public void setNextDataType(Attributes attributes) {
		nextDataType = CellDataType.NUMBER; // cellType为空，则表示该单元格类型为数字
		formatIndex = -1;
		formatString = null;
		String cellType = attributes.getValue("t"); // 单元格类型
		String cellStyleStr = attributes.getValue("s"); //
		String columnData = attributes.getValue("r"); // 获取单元格的位置，如A1,B1

		if ("b".equals(cellType)) { // 处理布尔值
			nextDataType = CellDataType.BOOL;
		} else if ("e".equals(cellType)) { // 处理错误
			nextDataType = CellDataType.ERROR;
		} else if ("inlineStr".equals(cellType)) {
			nextDataType = CellDataType.INLINESTR;
		} else if ("s".equals(cellType)) { // 处理字符串
			nextDataType = CellDataType.SSTINDEX;
		} else if ("str".equals(cellType)) {
			nextDataType = CellDataType.FORMULA;
		}

		if (cellStyleStr != null) { // 处理日期
			int styleIndex = Integer.parseInt(cellStyleStr);
			XSSFCellStyle style = stylesTable.getStyleAt(styleIndex);
			formatIndex = style.getDataFormat();

			formatString = style.getDataFormatString();

			if (formatString == null) {
				nextDataType = CellDataType.NULL;
				formatString = BuiltinFormats.getBuiltinFormat(formatIndex);
			} else {
				if (nextDataType == CellDataType.NUMBER && dateFormat!= null && ExcelDateUtil.isDateFormat(formatString)) {
					//nextDataType = CellDataType.DATE;
					formatString = dateFormat;
				}
				//System.out.println(formatString + ":" + nextDataType);
			}
		}
	}

	/**
	 * 对解析出来的数据进行类型处理
	 * 
	 * @param value   单元格的值， value代表解析：BOOL的为0或1，
	 *                ERROR的为内容值，FORMULA的为内容值，INLINESTR的为索引值需转换为内容值，
	 *                SSTINDEX的为索引值需转换为内容值， NUMBER为内容值，DATE为内容值
	 * @param thisStr 一个空字符串
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public String getDataValue(String value, String thisStr) {
		if ( totalRows==2293)
			System.out.println("");
		switch (nextDataType) {
		// 这几个的顺序不能随便交换，交换了很可能会导致数据错误
		case BOOL: // 布尔值
			char first = value.charAt(0);
			thisStr = first == '0' ? "FALSE" : "TRUE";
			break;
		case ERROR: // 错误
			// thisStr = "\"ERROR:" + value.toString() + '"';
			// 错误单元格包含类似的信息：ERROR:#DIV/0!，按空值处理
			thisStr = "";
			break;
		case FORMULA: // 公式
			thisStr = value.toString();
			break;
		case INLINESTR:
			XSSFRichTextString rtsi = new XSSFRichTextString(value.toString());
			thisStr = rtsi.toString();
			rtsi = null;
			break;
		case SSTINDEX: // 字符串
			thisStr = queryStringByIndex(value);
			break;
		case NUMBER: // 数字

			do {
				try {
					double dvalue = Double.parseDouble(value);
					if (formatString != null) {
						thisStr = formatter.formatRawCellContents(dvalue, formatIndex, formatString).trim();
						if (ExcelDateUtil.isValidDateValue(thisStr,formatString)) {
							break;
						}
					}
					if (numericFormat != null) {
						// 使用替代数字格式
						int fmtIndex = BuiltinFormats.getBuiltinFormat(numericFormat);
						thisStr = formatter.formatRawCellContents(dvalue, fmtIndex, numericFormat);
						if (thisStr.contains("E")) {
							// 不使用科学计数法
							BigDecimal bd = new BigDecimal(thisStr);
							thisStr = bd.toPlainString();
						}
						break;
					}
				} catch (NumberFormatException e) {
				}
				// 上述解析失败直接原值
				thisStr = value;
			} while (false);

			thisStr = thisStr.replace("_", "").trim();
			break;
		case DATE: 
			// 无论内容是什么，只要设为日期格式就会进到这里
			try {
				thisStr = formatter.formatRawCellContents(Double.parseDouble(value), formatIndex, formatString).trim();
				// TODO: 如果内容非日期但设置的是日期格式，需要按字符串处理，无法准确识别，暂用日期比较模糊判断。在Excel2003解析中没这个问题
				if (!ExcelDateUtil.isValidDateValue(thisStr,formatString)) {
					thisStr = queryStringByIndex(value);
				} else {
					// 对日期字符串作特殊处理，去掉T
					thisStr = thisStr.replace("T", " ");
				}
			} catch (NumberFormatException e) {
				// 如果value不是有效数字，则按字符串查询
				thisStr = queryStringByIndex(value);
			}
			break;
		default:
			thisStr = " ";
			break;
		}
		return thisStr;
	}

	private String queryStringByIndex(String sstIndex) {

		String thisStr;

		try {
			int idx = Integer.parseInt(sstIndex);
			XSSFRichTextString rtss = new XSSFRichTextString(sst.getEntryAt(idx));// 根据idx索引值获取内容值
			thisStr = rtss.toString();
			// System.out.println(thisStr);
			// 有些字符串是文本格式的，但内容却是日期

			rtss = null;
		} catch (NumberFormatException ex) {
			thisStr = sstIndex;
		}
		return thisStr;
	}

	public int countNullCell(String ref, String preRef) {
		// excel2007最大行数是1048576，最大列数是16384，最后一列列名是XFD
		String xfd = ref.replaceAll("\\d+", "");
		String xfd_1 = preRef.replaceAll("\\d+", "");

		xfd = fillChar(xfd, 3, '@', true);
		xfd_1 = fillChar(xfd_1, 3, '@', true);

		char[] letter = xfd.toCharArray();
		char[] letter_1 = xfd_1.toCharArray();
		int res = (letter[0] - letter_1[0]) * 26 * 26 + (letter[1] - letter_1[1]) * 26 + (letter[2] - letter_1[2]);
		return res - 1;
	}

	public String fillChar(String str, int len, char let, boolean isPre) {
		int len_1 = str.length();
		if (len_1 < len) {
			if (isPre) {
				for (int i = 0; i < (len - len_1); i++) {
					str = let + str;
				}
			} else {
				for (int i = 0; i < (len - len_1); i++) {
					str = str + let;
				}
			}
		}
		return str;
	}

	/**
	 * @return the exceptionMessage
	 */
	public String getExceptionMessage() {
		return exceptionMessage;
	}

	private boolean isReadSheet(int sheetIndex, String sheetName) {
		if (readSheetsIndex != null && readSheetsIndex.length > 0) {
			for (int index : readSheetsIndex) {
				if (sheetIndex == index)
					return true;
			}
			return false;
		} else if (readSheetsRegex != null && !readSheetsRegex.equals("")) {

			Pattern pa = Pattern.compile(readSheetsRegex);
			Matcher m = pa.matcher(sheetName);
			return m.find();
		}

		return true;
	}
}
