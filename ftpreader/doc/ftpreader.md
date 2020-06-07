# DataX FtpReader 说明


------------

## 1 快速介绍

FtpReader提供了读取远程FTP文件系统数据存储的能力。在底层实现上，FtpReader获取远程FTP文件数据，并转换为DataX传输协议传递给Writer。

**本地文件内容存放的是一张逻辑意义上的二维表，例如CSV格式的文本信息。**


## 2 功能与限制

FtpReader实现了从远程FTP文件读取数据并转为DataX协议的功能，远程FTP文件本身是无结构化数据存储，对于DataX而言，FtpReader实现上类比TxtFileReader，有诸多相似之处。目前FtpReader支持功能如下：

1. 支持且仅支持读取TXT和Excel（xls/xlsx）文件，且要求文件中shema为一张二维表。

2. 支持类CSV格式文件，自定义分隔符。

3. 支持多种类型数据读取(使用String表示)，支持列裁剪，支持列常量

4. 支持递归读取、支持文件名过滤。

5. 支持文本压缩，现有压缩格式为zip、gzip、bzip2。Excel文件不支持压缩

6. 多个File可以支持并发读取。

我们暂时不能做到：

1. 单个File支持多线程并发读取，这里涉及到单个File内部切分算法。二期考虑支持。

2.  单个File在压缩情况下，从技术上无法支持多线程并发读取。


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
                    "name": "ftpreader",
                    "parameter": {
                        "protocol": "sftp",
                        "host": "127.0.0.1",
                        "port": 22,
                        "username": "xx",
                        "password": "xxx",
                        "path": [
                            "/home/hanfa.shf/ftpReaderTest/data"
                        ],
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
                        "encoding": "UTF-8",
                        "fieldDelimiter": ","
                    }
                },
                "writer": {
                    "name": "ftpWriter",
                    "parameter": {
                        "path": "/home/hanfa.shf/ftpReaderTest/result",
                        "fileName": "shihf",
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

* **protocol**

	* 描述：ftp服务器协议，目前支持传输协议有ftp和sftp。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **host**

	* 描述：ftp服务器地址。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **port**

	* 描述：ftp服务器端口。 <br />

	* 必选：否 <br />

	* 默认值：若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21 <br />

* **timeout**

	* 描述：连接ftp服务器连接超时时间，单位毫秒。 <br />

	* 必选：否 <br />

	* 默认值：60000（1分钟）<br />
* **connectPattern**

	* 描述：连接模式（主动模式或者被动模式）。该参数只在传输协议是标准ftp协议时使用，值只能为：PORT (主动)，PASV（被动）。两种模式主要的不同是数据连接建立的不同。对于Port模式，是客户端在本地打开一个端口等服务器去连接建立数据连接，而Pasv模式就是服务器打开一个端口等待客户端去建立一个数据连接。<br />

	* 必选：否 <br />

	* 默认值：PASV<br />

* **username**

	* 描述：ftp服务器访问用户名。 <br />

	* 必选：无passwordFile必选 <br />

	* 默认值：无 <br />

* **password**

	* 描述：ftp服务器访问密码。 <br />

	* 必选：无passwordFile时必选 <br />

	* 默认值：无 <br />

* **passwordFile**

	* 描述：保存登录ftp服务器使用的用户名和密码的一个Properties文件，方便用户名密码配置及更换 <br />

	* 必选：否 <br />

	* 默认值：无 <br />
	
* **path**

	* 描述：远程FTP文件系统的路径信息，注意这里可以支持填写多个路径。 <br />

		当指定单个远程FTP文件，FtpReader暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个File可以进行多线程并发读取。

		当指定多个远程FTP文件，FtpReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。

		当指定通配符，FtpReader尝试遍历出多个文件信息。例如: 指定/*代表读取/目录下所有的文件，指定/bazhen/\*代表读取bazhen目录下游所有的文件。**FtpReader目前只支持\*作为文件通配符。**

		**特别需要注意的是，DataX会将一个作业下同步的所有Text File视作同一张数据表。用户必须自己保证所有的File能够适配同一套schema信息。读取文件用户必须保证为类CSV格式，并且提供给DataX权限可读。**

		**特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，DataX将报错。**

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **pathFilter**

	* 描述：匹配文件路径的正则表达式，包括文件夹及文件名两部分 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **readMode**

	* 描述：文件读取模式，支持三种模式：full,different,lastest
	    full 表示读取全部
	    different 只读取与状态数据库中记录的时间戳不同的文件
	    latest 只读取比状态数据库中记录的时间戳更新的文件 <br />

	* 必选：否 <br />

	* 默认值：full <br />
	
* **maxTraversalLevel**

	* 描述：允许遍历文件夹的最大层数。<br />

 	* 必选：否 <br />

 	* 默认值：100 <br />
	 
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
		
		用户可以指定Column字段信息，配置如下：

		```json
		{
           "type": "long",
           "index": 0    //从远程FTP文件文本第一列获取int字段
        },
        {
           "type": "string",
           "value": "alibaba"  //从FtpReader内部生成alibaba的字符串字段作为当前字段
        }
		```

		对于用户指定Column信息，type必须填写，index/value必须选择其一。

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />

* **skipHeader**

	* 描述：文件可能存在表头为标题情况，需要跳过。默认不跳过。<br />

 	* 必选：否 <br />

 	* 默认值：false <br />

* **nullFormat**

	* 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />

		 例如如果用户配置: nullFormat:"\N"，那么如果源头数据是"\N"，DataX视作null字段。

 	* 必选：否 <br />

 	* 默认值：\N <br />

* **binaryFile**

	* 描述：是否按二进制文件下载

 	* 必选：否 <br />

 	* 默认值：false <br />
 	
Excel文件配置参数:
* **sheetIndexs**

	* 描述：需要读取的sheet页索引数组，从1开始(仅限Excel文件)。<br />

 	* 必选：否 <br />

 	* 默认值：空 <br />
    
* **sheetNames**

	* 描述：需要读取的sheet页名称，支持正则(仅限Excel文件)。<br />

 	* 必选：否 <br />

 	* 默认值：空 <br />

* **headerLine**

	* 描述：表头所在行的行号(仅限Excel文件)。<br />

 	* 必选：否 <br />

 	* 默认值：1 <br />

文本文件配置参数:
* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	* 必选：否 <br />

	* 默认值：, <br />

* **fileSplitLine**

	* 描述：把文件分隔成两张逻辑表的一串字符，源文件某一行的内容开头部分与这个字符串相同即为匹配 <br />

	* 必选：否 <br />

	* 默认值：无, <br />
	
* **beforeSplitLine**

	* 描述：要读取的数据是文件分隔行之前的部分还是之后的部分 <br />

	* 必选：否 <br />

	* 默认值：true, <br />
	
* **compress**

	* 描述：文本文件压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、gzip、bzip2。 <br />

	* 必选：否 <br />

	* 默认值：没有压缩 <br />

* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />

* **csvReaderConfig**

	* 描述：读取CSV类型文件参数配置，Map类型。读取CSV类型文件使用的CsvReader进行读取，会有很多配置，不配置则使用默认值。<br />

 	* 必选：否 <br />
 
 	* 默认值：无 <br />

        
常见配置：

```json
"csvReaderConfig":{
        "safetySwitch": false,
        "skipEmptyRecords": false,
        "useTextQualifier": false
}
```

所有配置项及默认值,配置时 csvReaderConfig 的map中请**严格按照以下字段名字进行配置**：

```
boolean caseSensitive = true;
char textQualifier = 34;
boolean trimWhitespace = true;
boolean useTextQualifier = true;//是否使用csv转义字符
char delimiter = 44;//分隔符
char recordDelimiter = 0;
char comment = 35;
boolean useComments = false;
int escapeMode = 1;
boolean safetySwitch = true;//单列长度是否限制100000字符
boolean skipEmptyRecords = true;//是否跳过空行
boolean captureRawRecord = true;
```


### 3.3 类型转换

远程FTP文件本身不提供数据类型，该类型是DataX FtpReader定义：

| DataX 内部类型| 远程FTP文件 数据类型    |
| -------- | -----  |
|
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

* 远程FTP文件 Long是指远程FTP文件文本中使用整形的字符串表示形式，例如"19901219"。
* 远程FTP文件 Double是指远程FTP文件文本中使用Double的字符串表示形式，例如"3.1415"。
* 远程FTP文件 Boolean是指远程FTP文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* 远程FTP文件 Date是指远程FTP文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 4 性能报告



## 5 约束限制

略

## 6 FAQ

略

