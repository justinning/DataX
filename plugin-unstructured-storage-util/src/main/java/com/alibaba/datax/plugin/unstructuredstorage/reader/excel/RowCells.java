package com.alibaba.datax.plugin.unstructuredstorage.reader.excel;

import java.util.ArrayList;

public class RowCells<E> extends ArrayList<E> {

	private static final long serialVersionUID = -249384408593518438L;

	@Override
	public void add(int index, E element) {
		/**
		 * 拦截特殊的值便于调试
		 */
		
		/**
		if (element instanceof String) {
			if ("第 -1 页，共 1 页".equals(element)) {
				System.out.println();
			}
		}*/
		try {
			//如果索引对象已经存在，则更新，否则就添加到后面。
			if (super.size() == index) {
				super.add(index, element);
			} else if (super.size() > index) {
				super.set(index, element);
			} else {
				//如果索引越界则do nothing
			}
		} catch (IndexOutOfBoundsException e) {
			System.out.println(e.getMessage());
		}
	}
}
