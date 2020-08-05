package com.alibaba.datax.plugin.unstructuredstorage.reader.excel;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ExcelDateUtil {

	public static boolean isValidDateValue(String thisStr,String dateFormat) {
		if ( dateFormat == null)
			return false;
		
		try {
			SimpleDateFormat df = new SimpleDateFormat(dateFormat);
			Date dt = df.parse(thisStr);
			//Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
			//Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse("2040-12-31");
			//System.out.println(d1.getTime());  //-28800000
			//System.out.println(d2.getTime());  //2240496000000
			if (dt.getTime() > -28800000L && dt.getTime() < 2240496000000L) {
				return true;
			}
		} catch (Exception e) {
			//e.printStackTrace();
		}
		return false;
	}
	
	public static boolean isDateFormat(String formatString) {
		
		if (formatString != null && !"General".equals(formatString)) {
			//可能的格式有
			//yyyy"年"m"月";@
			//yyyy\\-mm\\-dd
			//yyyy-m-d
			//yyyy//mm//d
			formatString = formatString.replaceAll("(年|月|日|\\\\-|-|\\|//)", "/");
			formatString = formatString.replace("\"", "");  
			
			if (formatString.contains("m/d/yy") || formatString.contains("yy/mm/dd") || formatString.contains("yy/m/d")
					|| formatString.contains("dd/mmm/yy") || formatString.contains("yyyy/m")) {
				return true;
			}
		}
		return false;
	}
}
