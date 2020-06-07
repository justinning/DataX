package com.alibaba.datax.plugin.unstructuredstorage.reader;

import java.io.Serializable;

public class FileInfo implements Serializable {

	private static final long serialVersionUID = 4823848879102141298L;
	private String path;
	private Long lastModified;
	
	public FileInfo() {
		
	}
	public FileInfo(String path,Long lastModified) {
		this.path = path;
		this.lastModified = lastModified;
	}
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public Long getLastModified() {
		return lastModified;
	}
	public void setLastModified(Long lastModified) {
		this.lastModified = lastModified;
	}

}
