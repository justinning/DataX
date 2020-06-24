package com.alibaba.datax.plugin.unstructuredstorage.reader;

import java.io.Serializable;

public class FileInfo implements Serializable {

	private static final long serialVersionUID = 4823848879102141298L;
	private String path;
	private Long lastModified;
	private Long size;
	
	public FileInfo() {
		
	}
	public FileInfo(String path,Long lastModified,Long size) {
		this.path = path;
		this.lastModified = lastModified;
		this.size = size;
	}
	public FileInfo(FileInfo fileInfo) {
		this.path = fileInfo.getPath();
		this.lastModified = fileInfo.getLastModified();
		this.size = fileInfo.getSize();
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
	public Long getSize() {
		return size;
	}
	public void setSize(Long size) {
		this.size = size;
	}

}
