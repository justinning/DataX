package com.alibaba.datax.plugin.unstructuredstorage.reader.excel;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.hssf.eventusermodel.EventWorkbookBuilder;
import org.apache.poi.hssf.eventusermodel.FormatTrackingHSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingRowDummyRecord;
import org.apache.poi.hssf.model.HSSFFormulaParser;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BlankRecord;
import org.apache.poi.hssf.record.BoolErrRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.EOFRecord;
import org.apache.poi.hssf.record.ExtendedFormatRecord;
import org.apache.poi.hssf.record.FormatRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RowRecord;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.hssf.usermodel.HSSFDataFormatter;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.CellType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desc 代码基于 https://github.com/SwordfallYeung/POIExcel 修改
 * 
 *       增加： 1、错误公式按空值处理 2、可按传入的格式重设数值型单元格样式(如果单元格是文本型，则不起作用) 3、完善日期格式化
 **/
public class HSSFEventParser implements HSSFListener {

	private int minColums = -1;

	private POIFSFileSystem fs;

	/**
	 * 把第一行列名的长度作为列的总长
	 */
	private int totalColumns = 0;

	/**
	 * 总行数
	 */
	private int totalRows = 0;

	/**
	 * 数据
	 */
	private List<String[]> dataFrame = new ArrayList<>();

	/**
	 * 上一行row的序号
	 */
	private int lastRowNumber;

	/**
	 * 上一单元格的序号
	 */
	private int lastColumnNumber;

	/**
	 * 是否输出formula，还是它对应的值
	 */
	private boolean outputFormulaValues = true;

	private final String NORMAL_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
	private static final Logger LOG = LoggerFactory.getLogger(HSSFEventParser.class);
	/**
	 * 用于转换formulas
	 */
	private EventWorkbookBuilder.SheetRecordCollectingListener workbookBuildingListener;

	// excel2003工作簿
	private HSSFWorkbook stubWorkbook;

	private SSTRecord sstRecord;

	private FormatTrackingHSSFListener formatListener;

	private final HSSFDataFormatter formatter = new HSSFDataFormatter();

	// 表索引，需要读取的为true
	private Map<Integer, Boolean> sheetMap = new HashMap<>();
	private int currentSheet = -1;

	private int nextRow;

	private int nextColumn;

	private boolean outputNextStringRecord;

	// 当前行
	private int curRow = 0;

	private int headerLine = 0;

	// 存储一行记录所有单元格的容器
	private List<String> cellList = new RowCells<>();

	/**
	 * 判断整行是否为空行的标记
	 */
	private boolean notEmptyLine = false;

	private int[] headerIndexs = null;

	private int[] readSheetsIndex = null;
	private String readSheetsRegex = null;
	private String[] usecols = null;
	private boolean includeHeader = false;
	private String numericFormat = null;

	private List<Integer> extendedRecordFormatIndexList = new ArrayList<Integer>();
	private Map<Integer, String> formatRecordIndexMap = new HashMap<>();

	/**
	 * 遍历excel下所有的sheet
	 * 
	 * @param numericFormat TODO
	 * @throws Exception
	 */
	public List<String[]> process(InputStream inputStream, int headerLine, String[] usecols, int[] sheetsIndex,
			String sheetsRegex, boolean skipHeader, String numericFormat) throws Exception {

		readSheetsIndex = sheetsIndex;
		readSheetsRegex = sheetsRegex;

		this.headerLine = headerLine;
		this.usecols = usecols;
		this.includeHeader = !skipHeader;
		this.numericFormat = numericFormat;

		this.fs = new POIFSFileSystem(inputStream);
		MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
		formatListener = new FormatTrackingHSSFListener(listener);
		HSSFEventFactory factory = new HSSFEventFactory();
		HSSFRequest request = new HSSFRequest();
		if (outputFormulaValues) {
			request.addListenerForAllRecords(formatListener);
			workbookBuildingListener = new EventWorkbookBuilder.SheetRecordCollectingListener(formatListener);
		} else {
			workbookBuildingListener = new EventWorkbookBuilder.SheetRecordCollectingListener(formatListener);
			request.addListenerForAllRecords(workbookBuildingListener);
		}
		factory.processWorkbookEvents(request, fs);

		return dataFrame;
	}

	/**
	 * HSSFListener 监听方法，处理Record 处理每个单元格
	 * 
	 * @param record
	 */
	public void processRecord(Record record) {
		int thisRow = -1;
		int thisColumn = -1;
		String thisStr = null;
		String value = null;

		switch (record.getSid()) {

		case BOFRecord.sid:

			BOFRecord br = (BOFRecord) record;
			if (br.getType() == br.TYPE_WORKBOOK) {
				// ignored
				if (stubWorkbook == null && workbookBuildingListener != null) {
					stubWorkbook = workbookBuildingListener.getStubHSSFWorkbook();
				} else {
					LOG.error("Cannot create stub network. Formula Strings cannot be parsed");
				}
			} else {
				// WORKSHEET\MACRO\CHAR\VB_MODULE\etc 忽略
			}

			break;
		case BoundSheetRecord.sid:
			BoundSheetRecord bsr = (BoundSheetRecord) record;
			String sheetName = bsr.getSheetname();
			int sheetIndex = this.sheetMap.size();

			if (isReadSheet(sheetIndex + 1, sheetName)) {
				this.sheetMap.put(sheetIndex, true);
			} else {
				this.sheetMap.put(sheetIndex, false);
			}
			break;
		case RowRecord.sid:
			RowRecord rowRec = (RowRecord) record;
			// LOG.debug("Row found. Number of Cells: " + rowRec.getLastCol());
			if (this.currentSheet == -1 && rowRec.getRowNumber() == 0) {
				// special handling first sheet
				this.currentSheet = 0;
			} else if (this.currentSheet >= 0 && rowRec.getRowNumber() == 0) {
				this.currentSheet++; // start processing next sheet
			}
			break;
		case SSTRecord.sid:
			sstRecord = (SSTRecord) record;
			break;
		case BlankRecord.sid: // 单元格为空白

			if (!this.sheetMap.get(this.currentSheet)) {
				// if not then do nothing
				break;
			}
			BlankRecord brec = (BlankRecord) record;
			thisRow = brec.getRow();
			thisColumn = brec.getColumn();
			thisStr = "";
			cellList.add(thisColumn, thisStr);
			break;
		case BoolErrRecord.sid: // 单元格为布尔类型
			if (!this.sheetMap.get(this.currentSheet)) {
				// if not then do nothing
				break;
			}
			BoolErrRecord berec = (BoolErrRecord) record;
			thisRow = berec.getRow();
			thisColumn = berec.getColumn();
			thisStr = berec.getBooleanValue() + "";
			break;
		case FormulaRecord.sid:// 单元格为公式类型
			if (!this.sheetMap.get(this.currentSheet)) {
				// if not then do nothing
				break;
			}
			FormulaRecord frec = (FormulaRecord) record;
			thisRow = frec.getRow();
			thisColumn = frec.getColumn();
			double doubleValue = frec.getValue();

			if (outputFormulaValues) {
				String formatString = formatListener.getFormatString(frec);
				if (CellType.ERROR.getCode() == frec.getCachedResultType()) {
					// 包含错误的公式，按空值处理
					thisStr = "";
				}
				else if (Double.isNaN(doubleValue) || CellType.STRING.getCode() == frec.getCachedResultType() ) {
					outputNextStringRecord = true;
					nextRow = frec.getRow();
					nextColumn = frec.getColumn();
					return;
				} else {
					if (isDateFormat(formatString)) {
						formatString = NORMAL_DATE_FORMAT;
					}
					if (!NORMAL_DATE_FORMAT.equals(formatString) && numericFormat != null) {
						formatString = numericFormat;
					}
					int fmtIndex = BuiltinFormats.getBuiltinFormat(formatString);
					thisStr = this.formatter.formatRawCellContents(doubleValue, fmtIndex, formatString);
				}
			} else {
				thisStr = '"' + HSSFFormulaParser.toFormulaString(stubWorkbook, frec.getParsedExpression()) + '"';
			}

			cellList.add(thisColumn, thisStr);
			checkRowIsNull(thisStr);

			break;
		case StringRecord.sid: // 单元格中公式的字符串
			if (!this.sheetMap.get(this.currentSheet)) {
				// if not then do nothing
				break;
			}
			if (outputNextStringRecord) {
				StringRecord srec = (StringRecord) record;
				thisStr = srec.getString();
				thisColumn = nextColumn;
				outputNextStringRecord = false;
				cellList.add(thisColumn, thisStr);
				checkRowIsNull(thisStr);
			}
			break;
		case LabelRecord.sid:
			if (!this.sheetMap.get(this.currentSheet)) {
				// if not then do nothing
				break;
			}
			LabelRecord lrec = (LabelRecord) record;
			curRow = thisRow = lrec.getRow();
			thisColumn = lrec.getColumn();
			value = lrec.getValue().trim();
			value = value.equals("") ? "" : value;
			cellList.add(thisColumn, value);
			checkRowIsNull(value);
			break;
		case LabelSSTRecord.sid: // 单元格为字符串类型
			if (!this.sheetMap.get(this.currentSheet)) {
				// if not then do nothing
				break;
			}
			LabelSSTRecord lsrec = (LabelSSTRecord) record;
			curRow = thisRow = lsrec.getRow();
			thisColumn = lsrec.getColumn();
			if (sstRecord == null) {
				cellList.add(thisColumn, "");
			} else {
				value = sstRecord.getString(lsrec.getSSTIndex()).toString().trim();
				value = value.equals("") ? "" : value;

				cellList.add(thisColumn, value);
				checkRowIsNull(value);
			}
			break;
		case NumberRecord.sid: // 单元格为数字类型
			if (!this.sheetMap.get(this.currentSheet)) {
				// if not then do nothing
				break;
			}
			NumberRecord numrec = (NumberRecord) record;
			curRow = thisRow = numrec.getRow();
			thisColumn = numrec.getColumn();

			Double valueDouble = ((NumberRecord) numrec).getValue();
			String formatString = formatListener.getFormatString(numrec);

			if (isDateFormat(formatString)) {
				formatString = NORMAL_DATE_FORMAT;
			}
			if (formatString != null && (NORMAL_DATE_FORMAT.equals(formatString) || numericFormat == null)) {
				int formatIndex = formatListener.getFormatIndex(numrec);
				value = formatter.formatRawCellContents(valueDouble, formatIndex, formatString).trim();
				value = value.equals("") ? "" : value;
			} else if (numericFormat != null) {
				// 使用替代数字格式
				int fmtIndex = BuiltinFormats.getBuiltinFormat(numericFormat);
				value = formatter.formatRawCellContents(valueDouble, fmtIndex, numericFormat);
				if (value.contains("E")) {
					// 不使用科学计数法
					BigDecimal bd = new BigDecimal(value);
					value = bd.toPlainString();
				}
			} else {
				value = valueDouble.toString();
			}

			// 向容器加入列值
			cellList.add(thisColumn, value);
			checkRowIsNull(value);
			break;
		case ExtendedFormatRecord.sid:
			// LOG.debug("Found extended format record");
			ExtendedFormatRecord nfir = (ExtendedFormatRecord) record;
			this.extendedRecordFormatIndexList.add((int) nfir.getFormatIndex());
			break;
		case FormatRecord.sid:
			// LOG.debug("Found format record");
			FormatRecord fr = (FormatRecord) record;
			this.formatRecordIndexMap.put(fr.getIndexCode(), fr.getFormatString());
			break;
		default:
			// LOG.debug("Ignored record: "+record.getSid());
			break;
		}
		
		// this is an empty row in the Excel
		if (record instanceof MissingRowDummyRecord) { 
			MissingRowDummyRecord emptyRow = (MissingRowDummyRecord) record;
			// LOG.debug("Detected Empty row");
			if ((this.currentSheet == -1) && (emptyRow.getRowNumber() == 0)) {
				// special handling first sheet
				this.currentSheet = 1;
			} else if ((this.currentSheet >= 0) && (emptyRow.getRowNumber() == 0)) {
				this.currentSheet++; // start processing next sheet
			}
		}
		// 遇到新行的操作
		if (thisRow != -1 && thisRow != lastRowNumber) {
			lastColumnNumber = -1;
		}

		// 空值的操作
		if (record instanceof MissingCellDummyRecord) {
			MissingCellDummyRecord mc = (MissingCellDummyRecord) record;
			curRow = thisRow = mc.getRow();
			thisColumn = mc.getColumn();
			cellList.add(thisColumn, "");
		}

		// 更新行和列的值
		if (thisRow > -1)
			lastRowNumber = thisRow;
		if (thisColumn > -1)
			lastColumnNumber = thisColumn;

		// 行结束时的操作
		if (record instanceof LastCellOfRowDummyRecord) {
			if (minColums > 0) {
				// 列值重新置空
				if (lastColumnNumber == -1) {
					lastColumnNumber = 0;
				}
			}
			lastColumnNumber = -1;

			if (notEmptyLine && this.sheetMap.get(currentSheet) ) {
				
				if (curRow + 1 == headerLine) {
					totalColumns = cellList.size(); // 获取列数

					// 获得headers各字段对应的列索引
					if (usecols != null && usecols.length > 0 && headerIndexs == null) {

						headerIndexs = new int[usecols.length];
						for (int i = 0; i < usecols.length; i++) {
							headerIndexs[i] = cellList.indexOf(usecols[i]);
						}
					}
				}

				if ((includeHeader && curRow + 1 >= headerLine) || (!includeHeader && curRow + 1 > headerLine)) {

					// 2003版尾部为空单元格的，xls里面是以该行最后一个有值的单元格为结束标记的，尾部空单元格跳过，故需补全
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
					dataFrame.add(recordRow);
					totalRows++;
				}
				
			}
			// 清空容器
			cellList.clear();
			notEmptyLine = false;
		}
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

	/**
	 * 如果里面某个单元格含有值，则标识该行不为空行
	 * 
	 * @param value
	 */
	private void checkRowIsNull(String value) {
		if (value != null && !"".equals(value)) {
			notEmptyLine = true;
		}
	}

	private boolean isDateFormat(String formatString) {
		if (formatString != null) {
			formatString = formatString.replace("\\", "/");
			formatString = formatString.replace("-", "/");
			formatString = formatString.replace("//", "/");

			if (formatString.contains("m/d/yy") || formatString.contains("yy/mm/dd") || formatString.contains("yy/m/d")
					|| formatString.contains("dd/mmm/yy")) {
				return true;
			}
		}
		return false;
	}
}