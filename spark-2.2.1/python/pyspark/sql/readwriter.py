#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

if sys.version >= '3':
    basestring = unicode = str

from py4j.java_gateway import JavaClass

from pyspark import RDD, since, keyword_only
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.column import _to_seq
from pyspark.sql.types import *
from pyspark.sql import utils

__all__ = ["DataFrameReader", "DataFrameWriter"]


def to_str(value):
    """
    A wrapper over str(), but converts bool values to lower case strings.
    If None is given, just returns None, instead of converting it to string "None".
	一个str()的包装器，但是将bool值转换为小写字符串。
	如果给出的为None，则返回None，而不是将它转换为字符串“None”。
    """
    if isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return value
    else:
        return str(value)


class OptionUtils(object):

    def _set_opts(self, schema=None, **options):
        """
        Set named options (filter out those the value is None)
		设置命名选项(过滤掉值为None选项)
        """
        if schema is not None:
            self.schema(schema)
        for k, v in options.items():
            if v is not None:
                self.option(k, v)


class DataFrameReader(OptionUtils):
    """
    Interface used to load a :class:`DataFrame` from external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`spark.read`
    to access this.
	用于从自外部存储系统(例如文件系统、键值存储等等)加载一个`DataFrame`类。使用:函数:“spark.read” 来访问。

    .. versionadded:: 1.4
    """

    def __init__(self, spark):
        self._jreader = spark._ssql_ctx.read()
        self._spark = spark

    def _df(self, jdf):
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(jdf, self._spark)

    @since(1.4)
    def format(self, source):
        """Specifies the input data source format.
		指定输入数据源格式。

        :param source: string, name of the data source, e.g. 'json', 'parquet'.
						字符串类型，数据源名称

        >>> df = spark.read.format('json').load('python/test_support/sql/people.json')
        >>> df.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        self._jreader = self._jreader.format(source)
        return self

    @since(1.4)
    def schema(self, schema):
        """Specifies the input schema.
		指定输入模式

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.
		一些数据源(例如JSON)可以从数据中自动推断出输入模式。
		在这里通过指定模式，底层数据源可以跳过模式推理步骤，从而加快数据加载速度。
		
        :param schema: a :class:`pyspark.sql.types.StructType` object
        """
        from pyspark.sql import SparkSession
        if not isinstance(schema, StructType):
            raise TypeError("schema should be StructType")
        spark = SparkSession.builder.getOrCreate()
        jschema = spark._jsparkSession.parseDataType(schema.json())
        self._jreader = self._jreader.schema(jschema)
        return self

    @since(1.5)
    def option(self, key, value):
        """Adds an input option for the underlying data source.
		为基础数据源添加一个输入选项

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a timezone to be used to parse timestamps
                in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
		你可以设置如下选项：
				`timeZone`：设置一个表示时区的字符串，在JSON/CSV 类型的数据源或分区数值中，用于解析时间戳
							如果它没有设置，它使用默认值，本地时区会话
        """
        self._jreader = self._jreader.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds input options for the underlying data source.
		为基础数据源添加输入选项

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a timezone to be used to parse timestamps
                in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        for k in options:
            self._jreader = self._jreader.option(k, to_str(options[k]))
        return self

    @since(1.4)
    def load(self, path=None, format=None, schema=None, **options):
        """Loads data from a data source and returns it as a :class`DataFrame`.
		从数据源加载数据并将其作为一个DataFrame类返回。

        :param path: optional string or a list of string for file-system backed data sources.
					可选的，字符串或用于文件系统支持的数据源的字符串列表。
        :param format: optional string for format of the data source. Default to 'parquet'.
						可选的，用于数据源格式化的字符串。默认为“parquet”。
        :param schema: optional :class:`pyspark.sql.types.StructType` for the input schema.
						可选的, 输入模式
        :param options: all other string options
						其他的所有字符串选项

        >>> df = spark.read.load('python/test_support/sql/parquet_partitioned', opt1=True,
        ...     opt2=1, opt3='str')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]

        >>> df = spark.read.format('json').load(['python/test_support/sql/people.json',
        ...     'python/test_support/sql/people1.json'])
        >>> df.dtypes
        [('age', 'bigint'), ('aka', 'string'), ('name', 'string')]
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if isinstance(path, basestring):
            return self._df(self._jreader.load(path))
        elif path is not None:
            if type(path) != list:
                path = [path]
            return self._df(self._jreader.load(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        else:
            return self._df(self._jreader.load())

    @since(1.4)
    def json(self, path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None,
             multiLine=None):
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.
		加载JSON文件并返回结果:DataFrame类。

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.
		默认情况下支持JSON格式

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.
		如果没有指定模式`schema`参数，那么这个方法就会通过第一运行输入来确定输入模式。
		
        :param path: string represents path to the JSON dataset, or a list of paths,
                     or RDD of Strings storing JSON objects.
					 字符串类型，
        :param schema: an optional :class:`pyspark.sql.types.StructType` for the input schema.
        :param primitivesAsString: infers all primitive values as a string type. If None is set,
                                   it uses the default value, ``false``.
        :param prefersDecimal: infers all floating-point values as a decimal type. If the values
                               do not fit in decimal, then it infers them as doubles. If None is
                               set, it uses the default value, ``false``.
        :param allowComments: ignores Java/C++ style comment in JSON records. If None is set,
                              it uses the default value, ``false``.
        :param allowUnquotedFieldNames: allows unquoted JSON field names. If None is set,
                                        it uses the default value, ``false``.
        :param allowSingleQuotes: allows single quotes in addition to double quotes. If None is
                                        set, it uses the default value, ``true``.
        :param allowNumericLeadingZero: allows leading zeros in numbers (e.g. 00012). If None is
                                        set, it uses the default value, ``false``.
        :param allowBackslashEscapingAnyCharacter: allows accepting quoting of all character
                                                   using backslash quoting mechanism. If None is
                                                   set, it uses the default value, ``false``.
        :param mode: allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

                * ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted \
                 record, and puts the malformed string into a field configured by \
                 ``columnNameOfCorruptRecord``. To keep corrupt records, an user can set \
                 a string type field named ``columnNameOfCorruptRecord`` in an user-defined \
                 schema. If a schema does not have the field, it drops corrupt records during \
                 parsing. When inferring a schema, it implicitly adds a \
                 ``columnNameOfCorruptRecord`` field in an output schema.
                *  ``DROPMALFORMED`` : ignores the whole corrupted records.
                *  ``FAILFAST`` : throws an exception when it meets corrupted records.

        :param columnNameOfCorruptRecord: allows renaming the new field having malformed string
                                          created by ``PERMISSIVE`` mode. This overrides
                                          ``spark.sql.columnNameOfCorruptRecord``. If None is set,
                                          it uses the value specified in
                                          ``spark.sql.columnNameOfCorruptRecord``.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param multiLine: parse one record, which may span multiple lines, per file. If None is
                          set, it uses the default value, ``false``.

        >>> df1 = spark.read.json('python/test_support/sql/people.json')
        >>> df1.dtypes
        [('age', 'bigint'), ('name', 'string')]
        >>> rdd = sc.textFile('python/test_support/sql/people.json')
        >>> df2 = spark.read.json(rdd)
        >>> df2.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        self._set_opts(
            schema=schema, primitivesAsString=primitivesAsString, prefersDecimal=prefersDecimal,
            allowComments=allowComments, allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes, allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode, columnNameOfCorruptRecord=columnNameOfCorruptRecord, dateFormat=dateFormat,
            timestampFormat=timestampFormat, multiLine=multiLine)
        if isinstance(path, basestring):
            path = [path]
        if type(path) == list:
            return self._df(self._jreader.json(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        elif isinstance(path, RDD):
            def func(iterator):
                for x in iterator:
                    if not isinstance(x, basestring):
                        x = unicode(x)
                    if isinstance(x, unicode):
                        x = x.encode("utf-8")
                    yield x
            keyed = path.mapPartitions(func)
            keyed._bypass_serializer = True
            jrdd = keyed._jrdd.map(self._spark._jvm.BytesToString())
            return self._df(self._jreader.json(jrdd))
        else:
            raise TypeError("path can be only string, list or RDD")

    @since(1.4)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        :param tableName: string, name of the table.

        >>> df = spark.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.createOrReplaceTempView('tmpTable')
        >>> spark.read.table('tmpTable').dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.table(tableName))

    @since(1.4)
    def parquet(self, *paths):
        """Loads Parquet files, returning the result as a :class:`DataFrame`.

        You can set the following Parquet-specific option(s) for reading Parquet files:
            * ``mergeSchema``: sets whether we should merge schemas collected from all \
                Parquet part-files. This will override ``spark.sql.parquet.mergeSchema``. \
                The default value is specified in ``spark.sql.parquet.mergeSchema``.

        >>> df = spark.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.parquet(_to_seq(self._spark._sc, paths)))

    @ignore_unicode_prefix
    @since(1.6)
    def text(self, paths):
        """
        Loads text files and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.

        Each line in the text file is a new row in the resulting DataFrame.

        :param paths: string, or list of strings, for input path(s).

        >>> df = spark.read.text('python/test_support/sql/text-test.txt')
        >>> df.collect()
        [Row(value=u'hello'), Row(value=u'this')]
        """
        if isinstance(paths, basestring):
            paths = [paths]
        return self._df(self._jreader.text(self._spark._sc._jvm.PythonUtils.toSeq(paths)))

    @since(2.0)
    def csv(self, path, schema=None, sep=None, encoding=None, quote=None, escape=None,
            comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None,
            ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None,
            negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None,
            maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None,
            columnNameOfCorruptRecord=None, multiLine=None):
        """Loads a CSV file and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        :param path: string, or list of strings, for input path(s).
        :param schema: an optional :class:`pyspark.sql.types.StructType` for the input schema.
        :param sep: sets the single character as a separator for each field and value.
                    If None is set, it uses the default value, ``,``.
        :param encoding: decodes the CSV files by the given encoding type. If None is set,
                         it uses the default value, ``UTF-8``.
        :param quote: sets the single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If you would like to turn off quotations, you need to set an
                      empty string.
        :param escape: sets the single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``.
        :param comment: sets the single character used for skipping lines beginning with this
                        character. By default (None), it is disabled.
        :param header: uses the first line as names of columns. If None is set, it uses the
                       default value, ``false``.
        :param inferSchema: infers the input schema automatically from data. It requires one extra
                       pass over the data. If None is set, it uses the default value, ``false``.
        :param ignoreLeadingWhiteSpace: A flag indicating whether or not leading whitespaces from
                                        values being read should be skipped. If None is set, it
                                        uses the default value, ``false``.
        :param ignoreTrailingWhiteSpace: A flag indicating whether or not trailing whitespaces from
                                         values being read should be skipped. If None is set, it
                                         uses the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string. Since 2.0.1, this ``nullValue`` param
                          applies to all supported types including the string type.
        :param nanValue: sets the string representation of a non-number value. If None is set, it
                         uses the default value, ``NaN``.
        :param positiveInf: sets the string representation of a positive infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param negativeInf: sets the string representation of a negative infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param maxColumns: defines a hard limit of how many columns a record can have. If None is
                           set, it uses the default value, ``20480``.
        :param maxCharsPerColumn: defines the maximum number of characters allowed for any given
                                  value being read. If None is set, it uses the default value,
                                  ``-1`` meaning unlimited length.
        :param maxMalformedLogPerPartition: this parameter is no longer used since Spark 2.2.0.
                                            If specified, it is ignored.
        :param mode: allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

                * ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted \
                  record, and puts the malformed string into a field configured by \
                  ``columnNameOfCorruptRecord``. To keep corrupt records, an user can set \
                  a string type field named ``columnNameOfCorruptRecord`` in an \
                  user-defined schema. If a schema does not have the field, it drops corrupt \
                  records during parsing. When a length of parsed CSV tokens is shorter than \
                  an expected length of a schema, it sets `null` for extra fields.
                * ``DROPMALFORMED`` : ignores the whole corrupted records.
                * ``FAILFAST`` : throws an exception when it meets corrupted records.

        :param columnNameOfCorruptRecord: allows renaming the new field having malformed string
                                          created by ``PERMISSIVE`` mode. This overrides
                                          ``spark.sql.columnNameOfCorruptRecord``. If None is set,
                                          it uses the value specified in
                                          ``spark.sql.columnNameOfCorruptRecord``.
        :param multiLine: parse records, which may span multiple lines. If None is
                          set, it uses the default value, ``false``.

        >>> df = spark.read.csv('python/test_support/sql/ages.csv')
        >>> df.dtypes
        [('_c0', 'string'), ('_c1', 'string')]
        """
        self._set_opts(
            schema=schema, sep=sep, encoding=encoding, quote=quote, escape=escape, comment=comment,
            header=header, inferSchema=inferSchema, ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace, nullValue=nullValue,
            nanValue=nanValue, positiveInf=positiveInf, negativeInf=negativeInf,
            dateFormat=dateFormat, timestampFormat=timestampFormat, maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition, mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord, multiLine=multiLine)
        if isinstance(path, basestring):
            path = [path]
        return self._df(self._jreader.csv(self._spark._sc._jvm.PythonUtils.toSeq(path)))

    @since(1.5)
    def orc(self, path):
        """Loads ORC files, returning the result as a :class:`DataFrame`.

        .. note:: Currently ORC support is only available together with Hive support.

        >>> df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> df.dtypes
        [('a', 'bigint'), ('b', 'int'), ('c', 'int')]
        """
        if isinstance(path, basestring):
            path = [path]
        return self._df(self._jreader.orc(_to_seq(self._spark._sc, path)))

    @since(1.4)
    def jdbc(self, url, table, column=None, lowerBound=None, upperBound=None, numPartitions=None,
             predicates=None, properties=None):
        """
        Construct a :class:`DataFrame` representing the database table named ``table``
        accessible via JDBC URL ``url`` and connection ``properties``.
		构建一个`DataFrame`类，它表示一个数据库表名为‘table’可以通过JDBC UR`url`链接和连接属性进行。

        Partitions of the table will be retrieved in parallel if either ``column`` or
        ``predicates`` is specified. ``lowerBound`, ``upperBound`` and ``numPartitions``
        is needed when ``column`` is specified.
		如果有`column`或者`predicates`任何一个被指定，表的分区将被并行检索。那么`lowerBound`、`lowerBound`、`numPartitions`都必须被指定。

        If both ``column`` and ``predicates`` are specified, ``column`` will be used.
		如果`column`和`predicates`同时被指定，那么column`也将被指定。

        .. note:: Don't create too many partitions in parallel on a large cluster; \
        otherwise Spark might crash your external database systems.
		不要在一个大型集群上创建太多的并行分区，否则，Spark可能会破坏您的外部数据库系统

        :param url: a JDBC URL of the form ``jdbc:subprotocol:subname`` 一个JDBC url
        :param table: the name of the table 表名
        :param column: the name of an integer column that will be used for partitioning;
                       if this parameter is specified, then ``numPartitions``, ``lowerBound``
                       (inclusive), and ``upperBound`` (exclusive) will form partition strides
                       for generated WHERE clause expressions used to split the column
                       ``column`` evenly
        :param lowerBound: the minimum value of ``column`` used to decide partition stride
        :param upperBound: the maximum value of ``column`` used to decide partition stride
        :param numPartitions: the number of partitions
        :param predicates: a list of expressions suitable for inclusion in WHERE clauses;
                           each one defines one partition of the :class:`DataFrame`
						   一个适合包含在where字句中的表达式列表，每一代表了`DataFrame`的分区
        :param properties: a dictionary of JDBC database connection arguments. Normally at
                           least properties "user" and "password" with their corresponding values.
                           For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }
						   一个JDBC数据库连接参数的字典。通常情况下至少包含用户和密码两个属性
        :return: a DataFrame
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        if column is not None:
            assert lowerBound is not None, "lowerBound can not be None when ``column`` is specified"
            assert upperBound is not None, "upperBound can not be None when ``column`` is specified"
            assert numPartitions is not None, \
                "numPartitions can not be None when ``column`` is specified"
            return self._df(self._jreader.jdbc(url, table, column, int(lowerBound), int(upperBound),
                                               int(numPartitions), jprop))
        if predicates is not None:
            gateway = self._spark._sc._gateway
            jpredicates = utils.toJArray(gateway, gateway.jvm.java.lang.String, predicates)
            return self._df(self._jreader.jdbc(url, table, jpredicates, jprop))
        return self._df(self._jreader.jdbc(url, table, jprop))


class DataFrameWriter(OptionUtils):
    """
    Interface used to write a :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`DataFrame.write`
    to access this.

    .. versionadded:: 1.4
    """
    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx
        self._jwrite = df._jdf.write()

    def _sq(self, jsq):
        from pyspark.sql.streaming import StreamingQuery
        return StreamingQuery(jsq)

    @since(1.4)
    def mode(self, saveMode):
        """Specifies the behavior when data or table already exists.

        Options include:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        >>> df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        # At the JVM side, the default value of mode is already set to "error".
        # So, if the given saveMode is None, we will not call JVM-side's mode method.
        if saveMode is not None:
            self._jwrite = self._jwrite.mode(saveMode)
        return self

    @since(1.4)
    def format(self, source):
        """Specifies the underlying output data source.

        :param source: string, name of the data source, e.g. 'json', 'parquet'.

        >>> df.write.format('json').save(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self._jwrite = self._jwrite.format(source)
        return self

    @since(1.5)
    def option(self, key, value):
        """Adds an output option for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds output options for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    @since(1.4)
    def partitionBy(self, *cols):
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        :param cols: name of columns

        >>> df.write.partitionBy('year', 'month').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._spark._sc, cols))
        return self

    @since(1.4)
    def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        """Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        :param path: the path in a Hadoop supported file system
        :param format: the format used to save
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns
        :param options: all other string options

        >>> df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if path is None:
            self._jwrite.save()
        else:
            self._jwrite.save(path)

    @since(1.4)
    def insertInto(self, tableName, overwrite=False):
        """Inserts the content of the :class:`DataFrame` to the specified table.

        It requires that the schema of the class:`DataFrame` is the same as the
        schema of the table.

        Optionally overwriting any existing data.
        """
        self._jwrite.mode("overwrite" if overwrite else "append").insertInto(tableName)

    @since(1.4)
    def saveAsTable(self, name, format=None, mode=None, partitionBy=None, **options):
        """Saves the content of the :class:`DataFrame` as the specified table.

        In the case the table already exists, behavior of this function depends on the
        save mode, specified by the `mode` function (default to throwing an exception).
        When `mode` is `Overwrite`, the schema of the :class:`DataFrame` does not need to be
        the same as that of the existing table.

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param name: the table name
        :param format: the format used to save
        :param mode: one of `append`, `overwrite`, `error`, `ignore` (default: error)
        :param partitionBy: names of partitioning columns
        :param options: all other string options
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        self._jwrite.saveAsTable(name)

    @since(1.4)
    def json(self, path, mode=None, compression=None, dateFormat=None, timestampFormat=None):
        """Saves the content of the :class:`DataFrame` in JSON format
        (`JSON Lines text format or newline-delimited JSON <http://jsonlines.org/>`_) at the
        specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.

        >>> df.write.json(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(
            compression=compression, dateFormat=dateFormat, timestampFormat=timestampFormat)
        self._jwrite.json(path)

    @since(1.4)
    def parquet(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, snappy, gzip, and lzo).
                            This will override ``spark.sql.parquet.compression.codec``. If None
                            is set, it uses the value specified in
                            ``spark.sql.parquet.compression.codec``.

        >>> df.write.parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.parquet(path)

    @since(1.6)
    def text(self, path, compression=None):
        """Saves the content of the DataFrame in a text file at the specified path.

        :param path: the path in any Hadoop supported file system
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).

        The DataFrame must have only one column that is of string type.
        Each row becomes a new line in the output file.
        """
        self._set_opts(compression=compression)
        self._jwrite.text(path)

    @since(2.0)
    def csv(self, path, mode=None, compression=None, sep=None, quote=None, escape=None,
            header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None,
            timestampFormat=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None):
        """Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.

        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param sep: sets the single character as a separator for each field and value. If None is
                    set, it uses the default value, ``,``.
        :param quote: sets the single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If you would like to turn off quotations, you need to set an
                      empty string.
        :param escape: sets the single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``
        :param escapeQuotes: a flag indicating whether values containing quotes should always
                             be enclosed in quotes. If None is set, it uses the default value
                             ``true``, escaping all values containing a quote character.
        :param quoteAll: a flag indicating whether all values should always be enclosed in
                          quotes. If None is set, it uses the default value ``false``,
                          only escaping values containing a quote character.
        :param header: writes the names of columns as the first line. If None is set, it uses
                       the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param ignoreLeadingWhiteSpace: a flag indicating whether or not leading whitespaces from
                                        values being written should be skipped. If None is set, it
                                        uses the default value, ``true``.
        :param ignoreTrailingWhiteSpace: a flag indicating whether or not trailing whitespaces from
                                         values being written should be skipped. If None is set, it
                                         uses the default value, ``true``.

        >>> df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(compression=compression, sep=sep, quote=quote, escape=escape, header=header,
                       nullValue=nullValue, escapeQuotes=escapeQuotes, quoteAll=quoteAll,
                       dateFormat=dateFormat, timestampFormat=timestampFormat,
                       ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
                       ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace)
        self._jwrite.csv(path)

    @since(1.5)
    def orc(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        .. note:: Currently ORC support is only available together with Hive support.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, snappy, zlib, and lzo).
                            This will override ``orc.compress``. If None is set, it uses the
                            default value, ``snappy``.

        >>> orc_df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> orc_df.write.orc(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.orc(path)

    @since(1.4)
    def jdbc(self, url, table, mode=None, properties=None):
        """Saves the content of the :class:`DataFrame` to an external database table via JDBC.

        .. note:: Don't create too many partitions in parallel on a large cluster; \
        otherwise Spark might crash your external database systems.

        :param url: a JDBC URL of the form ``jdbc:subprotocol:subname``
        :param table: Name of the table in the external database.
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param properties: a dictionary of JDBC database connection arguments. Normally at
                           least properties "user" and "password" with their corresponding values.
                           For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self._jwrite.mode(mode).jdbc(url, table, jprop)


def _test():
    import doctest
    import os
    import tempfile
    import py4j
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession, Row
    import pyspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    try:
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    except py4j.protocol.Py4JError:
        spark = SparkSession(sc)

    globs['tempfile'] = tempfile
    globs['os'] = os
    globs['sc'] = sc
    globs['spark'] = spark
    globs['df'] = spark.read.parquet('python/test_support/sql/parquet_partitioned')
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    sc.stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()