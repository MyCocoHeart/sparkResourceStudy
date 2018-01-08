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

from __future__ import print_function
import sys
import warnings
from functools import reduce
from threading import RLock

if sys.version >= '3':
    basestring = unicode = str
    xrange = range
else:
    from itertools import imap as map

from pyspark import since
from pyspark.rdd import RDD, ignore_unicode_prefix
from pyspark.sql.catalog import Catalog
from pyspark.sql.conf import RuntimeConfig
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import Row, DataType, StringType, StructType, _verify_type, \
    _infer_schema, _has_nulltype, _merge_type, _create_converter, _parse_datatype_string
from pyspark.sql.utils import install_exception_handler

__all__ = ["SparkSession"]


def _monkey_patch_RDD(sparkSession):
    def toDF(self, schema=None, sampleRatio=None):
        """
        Converts current :class:`RDD` into a :class:`DataFrame`
		 将current：class：`RDD`转换为：class：`DataFrame`

        This is a shorthand for ``spark.createDataFrame(rdd, schema, sampleRatio)``
		这是``spark.createDataFrame(rdd, schema, sampleRatio)``的一个缩写

        :param schema: a :class:`pyspark.sql.types.StructType` or list of names of columns
						是一个:class:`pyspark.sql.types.StructType` 类或者列名称构成的列表
        :param samplingRatio: the sample ratio of rows used for inferring
						推断行时采用的采样率
        :return: a DataFrame

        >>> rdd.toDF().collect()
        [Row(name=u'Alice', age=1)]
        """
        return sparkSession.createDataFrame(self, schema, sampleRatio)

    RDD.toDF = toDF


class SparkSession(object):
    """The entry point to programming Spark with the Dataset and DataFrame API.
	使用数据集和DataFrame API编写Spark程序的入口点

    A SparkSession can be used create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.
	SparkSession可以被用来创建：class：`DataFrame`注册：class：`DataFrame`作为表格，在表上执行SQL，缓存表和读取实验文件。
    To create a SparkSession, use the following builder pattern:

    >>> spark = SparkSession.builder \\
    ...     .master("local") \\
    ...     .appName("Word Count") \\
    ...     .config("spark.some.config.option", "some-value") \\
    ...     .getOrCreate()

    .. autoattribute:: builder
       :annotation:
    """

    class Builder(object):
        """Builder for :class:`SparkSession`.
        """

        _lock = RLock()
        _options = {}

        @since(2.0)
        def config(self, key=None, value=None, conf=None):
            """Sets a config option. Options set using this method are automatically propagated to
            both :class:`SparkConf` and :class:`SparkSession`'s own configuration.
			设置一个配置选项，使用这个方法设置的配置选项自动传播到:class:`SparkConf` and :class:`SparkSession`的配置中
            两个：class：`SparkConf`和：class：`SparkSession`自己的配置。

            For an existing SparkConf, use `conf` parameter.
			对于已有的SparkConf，使用`conf`参数。


            >>> from pyspark.conf import SparkConf
            >>> SparkSession.builder.config(conf=SparkConf())
            <pyspark.sql.session...

            For a (key, value) pair, you can omit parameter names.
			对于（键，值）对，可以省略参数名称

            >>> SparkSession.builder.config("spark.some.config.option", "some-value")
            <pyspark.sql.session...

            :param key: a key name string for configuration property
						配置属性的键名称（字符串）
            :param value: a value for configuration property
						配置属性的值
            :param conf: an instance of :class:`SparkConf`
						类：`SparkConf`的一个实例
            """
			
			"""
			funcDetail:如果conf为None，说明没有已存在的SparkConf实例，则根据输入的键值对来配置相关属性；
					   如果conf不为None，则获取conf的所有属性并配置
			参考资料：with语句适用于对资源进行访问的场合，
					  它可以确保不管使用过程中是否发生异常，都会执行必要的“清理”操作，释放资源。
					  比如文件使用后自动关闭、线程中锁的自动获取和释放等。
			"""
            with self._lock:
                if conf is None:
                    self._options[key] = str(value)
                else:
                    for (k, v) in conf.getAll():
                        self._options[k] = v
                return self

        @since(2.0)
        def master(self, master):
            """Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
            to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone
            cluster.
			设置要连接的Spark主机URL，如本地运行的”local“，”local [4]“在本地以4核心运行，或者“spark：// master：7077”以独立集群模式运行Spark

            :param master: a url for spark master
            """
            return self.config("spark.master", master)

        @since(2.0)
        def appName(self, name):
            """Sets a name for the application, which will be shown in the Spark web UI.
			为应用程序设置一个名称，它将显示在Spark的Web UI中
			
            If no application name is set, a randomly generated name will be used.
			 如果没有给应用程序设置名称，则会使用随机生成的名称
            :param name: an application name
            """
            return self.config("spark.app.name", name)

        @since(2.0)
        def enableHiveSupport(self):
            """Enables Hive support, including connectivity to a persistent Hive metastore, support
            for Hive serdes, and Hive user-defined functions.
			启用Hive支持，包括连接到持续的Hive Metastore，支持Hive serdes和Hive的自定义函数
            """
            return self.config("spark.sql.catalogImplementation", "hive")

        @since(2.0)
        def getOrCreate(self):
            """Gets an existing :class:`SparkSession` or, if there is no existing one, creates a
            new one based on the options set in this builder.
			获取一个存在的`SparkSession`类，或者，如果没有现有的，则根据builder中的选项集创建一个新的。


            This method first checks whether there is a valid global default SparkSession, and if
            yes, return that one. If no valid global default SparkSession exists, the method
            creates a new SparkSession and assigns the newly created SparkSession as the global
            default.
			这个方法首先检查是否有一个有效的全局默认SparkSession，如果有，就返回现有的那一个。如果不存在有效的全局默认SparkSession，
			该方法就创建一个新的SparkSession并将新创建的SparkSession指定为全局默认的。

            >>> s1 = SparkSession.builder.config("k1", "v1").getOrCreate()
            >>> s1.conf.get("k1") == s1.sparkContext.getConf().get("k1") == "v1"
            True

            In case an existing SparkSession is returned, the config options specified
            in this builder will be applied to the existing SparkSession.
			如果返回了一个现有的SparkSession，则在builder中指定的配置选项将应用到现有的SparkSession中。

            >>> s2 = SparkSession.builder.config("k2", "v2").getOrCreate()
            >>> s1.conf.get("k1") == s2.conf.get("k1")
            True
            >>> s1.conf.get("k2") == s2.conf.get("k2")
            True
            """
            with self._lock:
                from pyspark.context import SparkContext
                from pyspark.conf import SparkConf
                session = SparkSession._instantiatedSession#实例化session
                if session is None or session._sc._jsc is None:#如果没有session实例，则创建Spark应用程序的配置对象，并完成相关配置
                    sparkConf = SparkConf()
                    for key, value in self._options.items():
                        sparkConf.set(key, value)
                    sc = SparkContext.getOrCreate(sparkConf)#通过spark的SparkContext来实例化到spark集群的连接
                    # This SparkContext may be an existing one.
					#这个SparkContext可能是一个现有的。
                    for key, value in self._options.items():
                        # we need to propagate the confs
                        # before we create the SparkSession. Otherwise, confs like
                        # warehouse path and metastore url will not be set correctly (
                        # these confs cannot be changed once the SparkSession is created).
						#我们需要在创建SparkSession之前广播这些配置。否则,相关配置，如仓库路径和元数据地址，就不会被正确设置( 一旦创建了SparkSession，就不能更改这些配置。
                        sc._conf.set(key, value)
                    session = SparkSession(sc)
                for key, value in self._options.items():
                    session._jsparkSession.sessionState().conf().setConfString(key, value)
                for key, value in self._options.items():
                    session.sparkContext._conf.set(key, value)
                return session

    builder = Builder()
    """A class attribute having a :class:`Builder` to construct :class:`SparkSession` instances"""

    _instantiatedSession = None

    @ignore_unicode_prefix
    def __init__(self, sparkContext, jsparkSession=None):
        """Creates a new SparkSession.
		创建一个新的SparkSession

        >>> from datetime import datetime
        >>> spark = SparkSession(sc)
        >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
        ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
        ...     time=datetime(2014, 8, 1, 14, 1, 5))])
        >>> df = allTypes.toDF()
        >>> df.createOrReplaceTempView("allTypes")
        >>> spark.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
        ...            'from allTypes where b and i > 0').collect()
        [Row((i + CAST(1 AS BIGINT))=2, (d + CAST(1 AS DOUBLE))=2.0, (NOT b)=False, list[1]=2, \
            dict[s]=0, time=datetime.datetime(2014, 8, 1, 14, 1, 5), a=1)]
        >>> df.rdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
        [(1, u'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        from pyspark.sql.context import SQLContext
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        if jsparkSession is None:
            jsparkSession = self._jvm.SparkSession(self._jsc.sc())
        self._jsparkSession = jsparkSession
        self._jwrapped = self._jsparkSession.sqlContext()
        self._wrapped = SQLContext(self._sc, self, self._jwrapped)
        _monkey_patch_RDD(self)#将current：class：`RDD`转换为：class：`DataFrame`
        install_exception_handler()#将异常处理程序连接到Py4j，它可以捕获Java中的一些SQL异常。
        # If we had an instantiated SparkSession attached with a SparkContext
        # which is stopped now, we need to renew the instantiated SparkSession.
        # Otherwise, we will use invalid SparkSession when we call Builder.getOrCreate.
		#如果我们有一个依赖于一个SparkContext的实例化的SparkSession且现在已经停止了，那么我们需要再新建一个实例化的SparkSession。
		#否则，当我们调用Builder.getOrCreate时，我们将使用一个无效的SparkSession。
        if SparkSession._instantiatedSession is None \
                or SparkSession._instantiatedSession._sc._jsc is None:
            SparkSession._instantiatedSession = self

    def _repr_html_(self):
        return """
            <div>
                <p><b>SparkSession - {catalogImplementation}</b></p>
                {sc_HTML}
            </div>
        """.format(
            catalogImplementation=self.conf.get("spark.sql.catalogImplementation"),
            sc_HTML=self.sparkContext._repr_html_()
        )

    @since(2.0)
    def newSession(self):
        """
        Returns a new SparkSession as new session, that has separate SQLConf,
        registered temporary views and UDFs, but shared SparkContext and
        table cache.
        """
        return self.__class__(self._sc, self._jsparkSession.newSession())

    @property
    @since(2.0)
    def sparkContext(self):
        """Returns the underlying :class:`SparkContext`."""
        return self._sc

    @property
    @since(2.0)
    def version(self):
        """The version of Spark on which this application is running."""
        return self._jsparkSession.version()

    @property
    @since(2.0)
    def conf(self):
        """Runtime configuration interface for Spark.

        This is the interface through which the user can get and set all Spark and Hadoop
        configurations that are relevant to Spark SQL. When getting the value of a config,
        this defaults to the value set in the underlying :class:`SparkContext`, if any.
        """
        if not hasattr(self, "_conf"):
            self._conf = RuntimeConfig(self._jsparkSession.conf())
        return self._conf

    @property
    @since(2.0)
    def catalog(self):
        """Interface through which the user may create, drop, alter or query underlying
        databases, tables, functions etc.
        """
        if not hasattr(self, "_catalog"):
            self._catalog = Catalog(self)
        return self._catalog

    @property
    @since(2.0)
    def udf(self):
        """Returns a :class:`UDFRegistration` for UDF registration.

        :return: :class:`UDFRegistration`
        """
        from pyspark.sql.context import UDFRegistration
        return UDFRegistration(self._wrapped)

    @since(2.0)
    def range(self, start, end=None, step=1, numPartitions=None):
        """
        Create a :class:`DataFrame` with single :class:`pyspark.sql.types.LongType` column named
        ``id``, containing elements in a range from ``start`` to ``end`` (exclusive) with
        step value ``step``.

        :param start: the start value
        :param end: the end value (exclusive)
        :param step: the incremental step (default: 1)
        :param numPartitions: the number of partitions of the DataFrame
        :return: :class:`DataFrame`

        >>> spark.range(1, 7, 2).collect()
        [Row(id=1), Row(id=3), Row(id=5)]

        If only one argument is specified, it will be used as the end value.

        >>> spark.range(3).collect()
        [Row(id=0), Row(id=1), Row(id=2)]
        """
        if numPartitions is None:
            numPartitions = self._sc.defaultParallelism

        if end is None:
            jdf = self._jsparkSession.range(0, int(start), int(step), int(numPartitions))
        else:
            jdf = self._jsparkSession.range(int(start), int(end), int(step), int(numPartitions))

        return DataFrame(jdf, self._wrapped)

    def _inferSchemaFromList(self, data):
        """
        Infer schema from list of Row or tuple.

        :param data: list of Row or tuple
        :return: :class:`pyspark.sql.types.StructType`
        """
        if not data:
            raise ValueError("can not infer schema from empty dataset")
        first = data[0]
        if type(first) is dict:
            warnings.warn("inferring schema from dict is deprecated,"
                          "please use pyspark.sql.Row instead")
        schema = reduce(_merge_type, map(_infer_schema, data))
        if _has_nulltype(schema):
            raise ValueError("Some of types cannot be determined after inferring")
        return schema

    def _inferSchema(self, rdd, samplingRatio=None):
        """
        Infer schema from an RDD of Row or tuple.

        :param rdd: an RDD of Row or tuple
        :param samplingRatio: sampling ratio, or no sampling (default)
        :return: :class:`pyspark.sql.types.StructType`
        """
        first = rdd.first()
        if not first:
            raise ValueError("The first row in RDD is empty, "
                             "can not infer schema")
        if type(first) is dict:
            warnings.warn("Using RDD of dict to inferSchema is deprecated. "
                          "Use pyspark.sql.Row instead")

        if samplingRatio is None:
            schema = _infer_schema(first)
            if _has_nulltype(schema):
                for row in rdd.take(100)[1:]:
                    schema = _merge_type(schema, _infer_schema(row))
                    if not _has_nulltype(schema):
                        break
                else:
                    raise ValueError("Some of types cannot be determined by the "
                                     "first 100 rows, please try again with sampling")
        else:
            if samplingRatio < 0.99:
                rdd = rdd.sample(False, float(samplingRatio))
            schema = rdd.map(_infer_schema).reduce(_merge_type)
        return schema

    def _createFromRDD(self, rdd, schema, samplingRatio):
        """
        Create an RDD for DataFrame from an existing RDD, returns the RDD and schema.
        """
        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchema(rdd, samplingRatio)
            converter = _create_converter(struct)
            rdd = rdd.map(converter)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif not isinstance(schema, StructType):
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        rdd = rdd.map(schema.toInternal)
        return rdd, schema

    def _createFromLocal(self, data, schema):
        """
        Create an RDD for DataFrame from a list or pandas.DataFrame, returns
        the RDD and schema.
        """
        # make sure data could consumed multiple times
        if not isinstance(data, list):
            data = list(data)

        if schema is None or isinstance(schema, (list, tuple)):
            struct = self._inferSchemaFromList(data)
            converter = _create_converter(struct)
            data = map(converter, data)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    struct.fields[i].name = name
                    struct.names[i] = name
            schema = struct

        elif not isinstance(schema, StructType):
            raise TypeError("schema should be StructType or list or None, but got: %s" % schema)

        # convert python objects to sql data
        data = [schema.toInternal(row) for row in data]
        return self._sc.parallelize(data), schema

    def _get_numpy_record_dtypes(self, rec):
        """
        Used when converting a pandas.DataFrame to Spark using to_records(), this will correct
        the dtypes of records so they can be properly loaded into Spark.
        :param rec: a numpy record to check dtypes
        :return corrected dtypes for a numpy.record or None if no correction needed
        """
        import numpy as np
        cur_dtypes = rec.dtype
        col_names = cur_dtypes.names
        record_type_list = []
        has_rec_fix = False
        for i in xrange(len(cur_dtypes)):
            curr_type = cur_dtypes[i]
            # If type is a datetime64 timestamp, convert to microseconds
            # NOTE: if dtype is datetime[ns] then np.record.tolist() will output values as longs,
            # conversion from [us] or lower will lead to py datetime objects, see SPARK-22417
            if curr_type == np.dtype('datetime64[ns]'):
                curr_type = 'datetime64[us]'
                has_rec_fix = True
            record_type_list.append((str(col_names[i]), curr_type))
        return record_type_list if has_rec_fix else None

    def _convert_from_pandas(self, pdf, schema):
        """
         Convert a pandas.DataFrame to list of records that can be used to make a DataFrame
         :return tuple of list of records and schema
        """
        # If no schema supplied by user then get the names of columns only
        if schema is None:
            schema = [str(x) for x in pdf.columns]

        # Convert pandas.DataFrame to list of numpy records
        np_records = pdf.to_records(index=False)

        # Check if any columns need to be fixed for Spark to infer properly
        if len(np_records) > 0:
            record_type_list = self._get_numpy_record_dtypes(np_records[0])
            if record_type_list is not None:
                return [r.astype(record_type_list).tolist() for r in np_records], schema

        # Convert list of numpy records to python lists
        return [r.tolist() for r in np_records], schema

    @since(2.0)
    @ignore_unicode_prefix
    def createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True):
        """
        Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.

        When ``schema`` is a list of column names, the type of each column
        will be inferred from ``data``.

        When ``schema`` is ``None``, it will try to infer the schema (column names and types)
        from ``data``, which should be an RDD of :class:`Row`,
        or :class:`namedtuple`, or :class:`dict`.

        When ``schema`` is :class:`pyspark.sql.types.DataType` or a datatype string, it must match
        the real data, or an exception will be thrown at runtime. If the given schema is not
        :class:`pyspark.sql.types.StructType`, it will be wrapped into a
        :class:`pyspark.sql.types.StructType` as its only field, and the field name will be "value",
        each record will also be wrapped into a tuple, which can be converted to row later.

        If schema inference is needed, ``samplingRatio`` is used to determined the ratio of
        rows used for schema inference. The first row will be used if ``samplingRatio`` is ``None``.

        :param data: an RDD of any kind of SQL data representation(e.g. row, tuple, int, boolean,
            etc.), or :class:`list`, or :class:`pandas.DataFrame`.
        :param schema: a :class:`pyspark.sql.types.DataType` or a datatype string or a list of
            column names, default is ``None``.  The data type string format equals to
            :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct type can
            omit the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use
            ``byte`` instead of ``tinyint`` for :class:`pyspark.sql.types.ByteType`. We can also use
            ``int`` as a short name for ``IntegerType``.
        :param samplingRatio: the sample ratio of rows used for inferring
        :param verifySchema: verify data types of every row against schema.
        :return: :class:`DataFrame`

        .. versionchanged:: 2.1
           Added verifySchema.

        >>> l = [('Alice', 1)]
        >>> spark.createDataFrame(l).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> spark.createDataFrame(l, ['name', 'age']).collect()
        [Row(name=u'Alice', age=1)]

        >>> d = [{'name': 'Alice', 'age': 1}]
        >>> spark.createDataFrame(d).collect()
        [Row(age=1, name=u'Alice')]

        >>> rdd = sc.parallelize(l)
        >>> spark.createDataFrame(rdd).collect()
        [Row(_1=u'Alice', _2=1)]
        >>> df = spark.createDataFrame(rdd, ['name', 'age'])
        >>> df.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql import Row
        >>> Person = Row('name', 'age')
        >>> person = rdd.map(lambda r: Person(*r))
        >>> df2 = spark.createDataFrame(person)
        >>> df2.collect()
        [Row(name=u'Alice', age=1)]

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...    StructField("name", StringType(), True),
        ...    StructField("age", IntegerType(), True)])
        >>> df3 = spark.createDataFrame(rdd, schema)
        >>> df3.collect()
        [Row(name=u'Alice', age=1)]

        >>> spark.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
        [Row(name=u'Alice', age=1)]
        >>> spark.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
        [Row(0=1, 1=2)]

        >>> spark.createDataFrame(rdd, "a: string, b: int").collect()
        [Row(a=u'Alice', b=1)]
        >>> rdd = rdd.map(lambda row: row[1])
        >>> spark.createDataFrame(rdd, "int").collect()
        [Row(value=1)]
        >>> spark.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")

        if isinstance(schema, basestring):
            schema = _parse_datatype_string(schema)

        try:
            import pandas
            has_pandas = True
        except Exception:
            has_pandas = False
        if has_pandas and isinstance(data, pandas.DataFrame):
            data, schema = self._convert_from_pandas(data, schema)

        verify_func = _verify_type if verifySchema else lambda _, t: True
        if isinstance(schema, StructType):
            def prepare(obj):
                verify_func(obj, schema)
                return obj
        elif isinstance(schema, DataType):
            dataType = schema
            schema = StructType().add("value", schema)

            def prepare(obj):
                verify_func(obj, dataType)
                return obj,
        else:
            if isinstance(schema, list):
                schema = [x.encode('utf-8') if not isinstance(x, str) else x for x in schema]
            prepare = lambda obj: obj

        if isinstance(data, RDD):
            rdd, schema = self._createFromRDD(data.map(prepare), schema, samplingRatio)
        else:
            rdd, schema = self._createFromLocal(map(prepare, data), schema)
        jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
        jdf = self._jsparkSession.applySchemaToPythonRDD(jrdd.rdd(), schema.json())
        df = DataFrame(jdf, self._wrapped)
        df._schema = schema
        return df

    @ignore_unicode_prefix
    @since(2.0)
    def sql(self, sqlQuery):
        """Returns a :class:`DataFrame` representing the result of the given query.
		返回一个DataFrame类，它表示给定查询的结果

        :return: :class:`DataFrame`

        >>> df.createOrReplaceTempView("table1")
        >>> df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> df2.collect()
        [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]
        """
        return DataFrame(self._jsparkSession.sql(sqlQuery), self._wrapped)

    @since(2.0)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.
		返回指定的表为:DataFrame类

        :return: :class:`DataFrame`

        >>> df.createOrReplaceTempView("table1")
        >>> df2 = spark.table("table1")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        return DataFrame(self._jsparkSession.table(tableName), self._wrapped)

    @property
    @since(2.0)
    def read(self):
        """
        Returns a :class:`DataFrameReader` that can be used to read data
        in as a :class:`DataFrame`.
		返回`DataFrameReader`类:可以用来在一个‘DataFrame’类中读取数据。

        :return: :class:`DataFrameReader`
        """
        return DataFrameReader(self._wrapped)

    @property
    @since(2.0)
    def readStream(self):
        """
        Returns a :class:`DataStreamReader` that can be used to read data streams
        as a streaming :class:`DataFrame`.

        .. note:: Evolving.

        :return: :class:`DataStreamReader`
        """
        return DataStreamReader(self._wrapped)

    @property
    @since(2.0)
    def streams(self):
        """Returns a :class:`StreamingQueryManager` that allows managing all the
        :class:`StreamingQuery` StreamingQueries active on `this` context.

        .. note:: Evolving.

        :return: :class:`StreamingQueryManager`
        """
        from pyspark.sql.streaming import StreamingQueryManager
        return StreamingQueryManager(self._jsparkSession.streams())

    @since(2.0)
    def stop(self):
        """Stop the underlying :class:`SparkContext`.
        """
        self._sc.stop()
        SparkSession._instantiatedSession = None

    @since(2.0)
    def __enter__(self):
        """
        Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.
        """
        return self

    @since(2.0)
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Enable 'with SparkSession.builder.(...).getOrCreate() as session: app' syntax.

        Specifically stop the SparkSession on exit of the with block.
        """
        self.stop()


def _test():
    import os
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row
    import pyspark.sql.session

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.session.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['spark'] = SparkSession(sc)
    globs['rdd'] = rdd = sc.parallelize(
        [Row(field1=1, field2="row1"),
         Row(field1=2, field2="row2"),
         Row(field1=3, field2="row3")])
    globs['df'] = rdd.toDF()
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.session, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()