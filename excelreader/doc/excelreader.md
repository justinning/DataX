# DataX ExcelReader 说明


------------

## 1 快速介绍

ExcelReader提供了读取本地文件系统数据存储的能力。在底层实现上，ExcelReader获取本地文件数据，并转换为DataX传输协议传递给Writer。

**本地文件内容存放的是一张逻辑意义上的二维表。**


## 2 功能与限制

ExcelReader实现了从本地文件读取数据并转为DataX协议的功能，本地文件本身是无结构化数据存储，对于DataX而言，ExcelReader实现上类比OSSReader，有诸多相似之处。目前ExcelReader支持功能如下：

1. 支持且仅支持读取XLS和XLSX的文件，且要求文件中shema为一张二维表。

2. 支持递归读取、支持文件名过滤。

3. 多个File可以支持并发读取。

限制：

1. 不支持从压缩包中读取Excel文件。

## 3 功能说明


### 3.1 配置样例

```json
{
    "setting": {},
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            {
                "reader": {
                    "name": "excelreader",
                    "parameter": {
                        "path": ["/home/justin/case00/data"],
                        "column": [
                            {
                                "index": 0,
                                "type": "long"
                            },
                            {
                                "index": 1,
                                "type": "boolean"
                            },
                            {
                                "index": 2,
                                "type": "double"
                            },
                            {
                                "index": 3,
                                "type": "string"
                            },
                            {
                                "index": 4,
                                "type": "date",
                                "format": "yyyy.MM.dd"
                            }
                        ],
                        "headerLine": 1,
                        "skipHeader": true
                    }
                },
                "writer": {
                    "name": "txtfilewriter",
                    "parameter": {
                        "path": "/home/justin/case00/result",
                        "fileName": "luohw",
                        "writeMode": "truncate",
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明

* **path**

	* 描述：本地文件系统的路径信息，注意这里可以支持填写多个路径。 <br />

		 当指定单个本地文件，ExcelReader暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个File可以进行多线程并发读取。

		当指定多个本地文件，ExcelReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。

		当指定通配符，ExcelReader尝试遍历出多个文件信息。例如: 指定/*代表读取/目录下所有的文件，指定/bazhen/\*代表读取bazhen目录下游所有的文件。**ExcelReader目前只支持\*作为文件通配符。**

		**特别需要注意的是，DataX会将一个作业下同步的所有Text File视作同一张数据表。用户必须自己保证所有的File能够适配同一套schema信息。读取文件用户必须保证为类CSV格式，并且提供给DataX权限可读。**

		**特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，DataX将报错。**

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：读取字段列表，type指定源数据的类型，index指定当前列来自于文本第几列(以0开始)，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。 <br />

		默认情况下，用户可以全部按照String类型读取数据，配置如下：

		```json
			"column": ["*"]
		```
		用户可以指定Column字段名称，只读取指定字段的值，未找到的字段值为null，配置如下：
		```json
			"column": [
			   "字段1",
			   "字段2",
			   "字段3"
		    ]
		```
		
		用户也可以指定Column字段信息，配置如下：

		```json
		{
           "type": "long",
           "index": 0    //从本地文件文本第一列获取int字段
        },
        {
           "type": "string",
           "value": "alibaba"  //从ExcelReader内部生成alibaba的字符串字段作为当前字段
        }
		```

		对于用户指定Column信息，type必须填写，index/value必须选择其一。

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />

* **sheetIndexs**

	* 描述：需要读取的sheet页索引数组，从1开始。<br />

 	* 必选：否 <br />

 	* 默认值：空 <br />
    
* **sheetNames**

	* 描述：需要读取的sheet页名称，支持正则。<br />

 	* 必选：否 <br />

 	* 默认值：空 <br />

* **headerLine**

	* 描述：表头所在行的行号。<br />

 	* 必选：否 <br />

 	* 默认值：1 <br />
 	
* **skipHeader**

	* 描述：类CSV格式文件可能存在表头为标题情况，需要跳过。默认不跳过。<br />

 	* 必选：否 <br />

 	* 默认值：false <br />

* **readFileAttrs**

	* 描述：在读取的记录最后补充两个字段：源文件路径(__filepath)及记录所在的文件行号(__line)，可用于血缘追溯。<br />

 	* 必选：否 <br />

 	* 默认值：false <br />

### 3.3 类型转换

本地文件本身不提供数据类型，该类型是DataX ExcelReader定义：

| DataX 内部类型| 本地文件 数据类型    |
| -------- | -----  |
|
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

* 本地文件 Long是指本地文件文本中使用整形的字符串表示形式，例如"19901219"。
* 本地文件 Double是指本地文件文本中使用Double的字符串表示形式，例如"3.1415"。
* 本地文件 Boolean是指本地文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* 本地文件 Date是指本地文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 4 性能报告



## 5 约束限制

略

## 6 FAQ

略


