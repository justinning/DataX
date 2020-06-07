package com.alibaba.datax.plugin.unstructuredstorage.reader;

public class Constant {
	public static final String DEFAULT_ENCODING = "UTF-8";

	public static final char DEFAULT_FIELD_DELIMITER = ',';

	public static final boolean DEFAULT_SKIP_HEADER = false;

	public static final String DEFAULT_NULL_FORMAT = "\\N";
	
    public static final Integer DEFAULT_BUFFER_SIZE = 8192;
    
    public static final String FIELD_NAME_FILEPATH = "__filepath";
    
    public static final String FIELD_NAME_LINE = "__line";
  
    public static final String READ_FULL = "full";

    public static final String READ_DIFFERENT = "different";
    
    public static final String READ_LATEST = "latest";
}
