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

import os
import shutil
import signal
import sys
import threading
import warnings
from threading import RLock
from tempfile import NamedTemporaryFile

from py4j.protocol import Py4JError

from pyspark import accumulators
from pyspark.accumulators import Accumulator
from pyspark.broadcast import Broadcast, BroadcastPickleRegistry
from pyspark.conf import SparkConf
from pyspark.files import SparkFiles
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer, \
    PairDeserializer, AutoBatchedSerializer, NoOpSerializer
from pyspark.storagelevel import StorageLevel
from pyspark.rdd import RDD, _load_from_socket, ignore_unicode_prefix
from pyspark.traceback_utils import CallSite, first_spark_call
from pyspark.status import StatusTracker
from pyspark.profiler import ProfilerCollector, BasicProfiler

if sys.version > '3':
    xrange = range


__all__ = ['SparkContext']


# These are special default configs for PySpark, they will overwrite
# the default ones for Spark if they are not configured by user.
DEFAULT_CONFIGS = {
    "spark.serializer.objectStreamReset": 100,
    "spark.rdd.compress": True,
}


class SparkContext(object):

    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create L{RDD} and
    broadcast variables on that cluster.
	Spark功能的主要入口点。SparkContext表示连到Spark群集的连接，可用来创建L{RDD}和在该群集上广播变量。
    """

    _gateway = None
    _jvm = None
    _next_accum_id = 0
    _active_spark_context = None
    _lock = RLock()
    _python_includes = None  # zip and egg 文件需要被添加到PYTHONPATH中

    PACKAGE_EXTENSIONS = ('.zip', '.egg', '.jar')

    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None,
                 environment=None, batchSize=0, serializer=PickleSerializer(), conf=None,
                 gateway=None, jsc=None, profiler_cls=BasicProfiler):
        """
        Create a new SparkContext. At least the master and app name should be set,
        either through the named parameters here or through C{conf}.
		创建一个新的SparkContext。要么通过这里的参数名称进行配置，要么通过C{conf}配置，
		不论哪种情况，至少需要设置master和应用程序名称。

        :param master: Cluster URL to connect to（连接到的集群URL）
               (e.g. mesos://host:port, spark://host:port, local[4]).
        :param appName: A name for your job, to display on the cluster web UI.
						作业的名称，显示在集群Web UI上
        :param sparkHome: Location where Spark is installed on cluster nodes.
						  Spark在群集节点上的安装位置
        :param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
			   要发送到群集的.zip或.py文件的集合 并添加到PYTHONPATH。
			   这些路径可以是本地文件系统或HDFS，HTTP，HTTPS或FTP URL。
        :param environment: A dictionary of environment variables to set on
               worker nodes.
			   要在工作节点上设置的环境变量字典。
        :param batchSize: The number of Python objects represented as a single
               Java object. Set 1 to disable batching, 0 to automatically choose
               the batch size based on object sizes, or -1 to use an unlimited
               batch size
			   表示为单个Java对象所需要的Python对象数量。设置1禁用批处理，0根据对象大小自动选择块大小，或-1不限制块大小
        :param serializer: The serializer for RDDs.
							RDD的序列化对象
        :param conf: A L{SparkConf} object setting Spark properties.
					一个用于设置Spark属性的{SparkConf}对象
        :param gateway: Use an existing gateway and JVM, otherwise a new JVM
               will be instantiated.
					使用现有的网关和JVM，否则一个新的JVM将被实例化。
        :param jsc: The JavaSparkContext instance (optional).
					JavaSparkContext实例（可选）
        :param profiler_cls: A class of custom Profiler used to do profiling
               (default is pyspark.profiler.BasicProfiler).
			   用于分析的一个自定义分析器（默认是pyspark.profiler.BasicProfiler）。


        >>> from pyspark.context import SparkContext
        >>> sc = SparkContext('local', 'test')

        >>> sc2 = SparkContext('local', 'test2') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError:...
        """
        self._callsite = first_spark_call() or CallSite(None, None, None)#检查异常信息
		#检查SparkContext是否被初始化，如果SparkContext已经在运行，则会引发异常
        SparkContext._ensure_initialized(self, gateway=gateway, conf=conf) 
        try:
            self._do_init(master, appName, sparkHome, pyFiles, environment, batchSize, serializer,
                          conf, jsc, profiler_cls)
        except:
            # If an error occurs, clean up in order to allow future SparkContext creation:
			#如果出现了错误，为了以后的SparkContext创建可以创建需要做相关清理
            self.stop()
            raise

    def _do_init(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer,
                 conf, jsc, profiler_cls):
        self.environment = environment or {}#要在工作节点上设置的环境变量
        # java gateway must have been launched at this point.
		#这个时候java的网关必须已经启动
        if conf is not None and conf._jconf is not None:
            # conf has been initialized in JVM properly, so use conf directly. This represent the
            # scenario that JVM has been launched before SparkConf is created (e.g. SparkContext is
            # created and then stopped, and we create a new SparkConf and new SparkContext again)
			#conf已在JVM上被正确初始化，所以直接使用的conf。
			#这说明在创建SparkConf之前已经启动了JVM（比如SparkContext被创建了，然后又被停止了，那么我们再创建一个新的SparkConf和新的SparkContext）
            self._conf = conf
        else:
            self._conf = SparkConf(_jvm=SparkContext._jvm)
            if conf is not None:
                for k, v in conf.getAll():
                    self._conf.set(k, v)

        self._batchSize = batchSize  # -1 represents an unlimited batch size ：-1表示没有限制块大小
        self._unbatched_serializer = serializer #没有分块的序列化对象
        if batchSize == 0:#如果块大小为0（不知道分块大小）则将序列化对象自动分块，否则为已分块的序列化对象
            self.serializer = AutoBatchedSerializer(self._unbatched_serializer)
        else:
            self.serializer = BatchedSerializer(self._unbatched_serializer,
                                                batchSize)

        # Set any parameters passed directly to us on the conf 设置在conf上直接传给我们的参数
        if master:
            self._conf.setMaster(master)#配置master属性
        if appName:
            self._conf.setAppName(appName)#配置appName属性
        if sparkHome:
            self._conf.setSparkHome(sparkHome)#配置sparkHome属性
        if environment:#配置环境
            for key, value in environment.items():
                self._conf.setExecutorEnv(key, value)
        for key, value in DEFAULT_CONFIGS.items():
            self._conf.setIfMissing(key, value)

        # Check that we have at least the required parameters 
		#检查我们是否有必须具备的参数,从这里可以看出master和appName属性是最小配置，是必须要配置的
        if not self._conf.contains("spark.master"):#检查是否配置了master属性，如果没有就抛出异常提示
            raise Exception("A master URL must be set in your configuration")
        if not self._conf.contains("spark.app.name"):#检查是否配置了appName属性，如果没有就抛出异常提示
            raise Exception("An application name must be set in your configuration")

        # Read back our properties from the conf in case we loaded some of them from
        # the classpath or an external config file
		#从conf中读回我们需要的属性，以防我们从其他的一些classpath或者外部的配置文件中加载这些属性并被覆盖
        self.master = self._conf.get("spark.master")
        self.appName = self._conf.get("spark.app.name")
        self.sparkHome = self._conf.get("spark.home", None)

        for (k, v) in self._conf.getAll():
            if k.startswith("spark.executorEnv."):
                varName = k[len("spark.executorEnv."):]
                self.environment[varName] = v

        self.environment["PYTHONHASHSEED"] = os.environ.get("PYTHONHASHSEED", "0")

        # Create the Java SparkContext through Py4J
		#通过Py4J创建Java SparkContext
        self._jsc = jsc or self._initialize_context(self._conf._jconf)
        # Reset the SparkConf to the one actually used by the SparkContext in JVM.
		#将SparkConf重置为JVM中SparkContext实际使用的那个。
        self._conf = SparkConf(_jconf=self._jsc.sc().conf())

        # Create a single Accumulator in Java that we'll send all our updates through;
        # they will be passed back to us through a TCP server
		#用Java创建一个单一的Accumulator，以便我们发送所有更新
		#他们将通过TCP服务器传回给我们
        self._accumulatorServer = accumulators._start_update_server()
        (host, port) = self._accumulatorServer.server_address
        self._javaAccumulator = self._jvm.PythonAccumulatorV2(host, port)
        self._jsc.sc().register(self._javaAccumulator)

        self.pythonExec = os.environ.get("PYSPARK_PYTHON", 'python')#python执行器的目录
        self.pythonVer = "%d.%d" % sys.version_info[:2]#python的版本

        if sys.version_info < (2, 7):#如果python版本小于2.7则给出警告：提示spark2.0.0已经不支持python2.6了
            warnings.warn("Support for Python 2.6 is deprecated as of Spark 2.0.0")

        # Broadcast's __reduce__ method stores Broadcast instances here.广播的__reduce__方法在这里存储广播实例。
        # This allows other code to determine which Broadcast instances have
        # been pickled, so it can determine which Java broadcast objects to
        # send.
		#它允许其他代码来确定哪些广播实例已经被序列化了，所以它可以确定发送哪个Java广播对象
        self._pickled_broadcast_vars = BroadcastPickleRegistry()#在Thread-local中注册已被序列化的广播变量

        SparkFiles._sc = self
        root_dir = SparkFiles.getRootDirectory()
        sys.path.insert(1, root_dir)#sys.path是个列表，用sys.path.append在末尾添加目录，当这个append执行完之后，新目录即时起效

        # Deploy any code dependencies specified in the constructor
		#部署构造函数中指定的任何代码依赖关系
        self._python_includes = list()
        for path in (pyFiles or []):#为以后在这个SparkContext上执行的所有任务添加需要的.py或.zip依赖项。						
            self.addPyFile(path)#{path}可以是本地的文件，HDFS（或其他Hadoop支持的文件系统）中的文件，或者HTTP，HTTPS或FTP URI。

        # Deploy code dependencies set by spark-submit; these will already have been added
        # with SparkContext.addFile, so we just need to add them to the PYTHONPATH
		#部署由spark-submit设置的代码依赖项; 
		#这些依赖需要已经被SparkContext.addFile方法添加进去了，所以我们只需要将它们添加到PYTHONPATH中就可以了
        for path in self._conf.get("spark.submit.pyFiles", "").split(","):#通过逗号分割所需要的文件目录
            if path != "":#如果文件不为空，则分离出文件的目录和文件名字
                (dirname, filename) = os.path.split(path)
                if filename[-4:].lower() in self.PACKAGE_EXTENSIONS:#如果文件后缀在('.zip', '.egg', '.jar')中，则将该文件名添加到相关依赖的列表中
                    self._python_includes.append(filename)
                    sys.path.insert(1, os.path.join(SparkFiles.getRootDirectory(), filename))#把下载后的文件（目录/文件名）添加到第1个位置上

        # Create a temporary directory inside spark.local.dir:
		#在spark.local.dir中创建一个临时目录
        local_dir = self._jvm.org.apache.spark.util.Utils.getLocalDir(self._jsc.sc().conf())
        self._temp_dir = \
            self._jvm.org.apache.spark.util.Utils.createTempDir(local_dir, "pyspark") \
                .getAbsolutePath()

        # profiling stats collected for each PythonRDD
		#为每个PythonRDD解析stats
        if self._conf.get("spark.python.profile", "false") == "true":
            dump_path = self._conf.get("spark.python.profile.dump", None)
            self.profiler_collector = ProfilerCollector(profiler_cls, dump_path)#在每个基础的stage中跟踪不同的解析器。
        else:
            self.profiler_collector = None

        # create a signal handler which would be invoked on receiving SIGINT
		#创建一个信号处理程序，它将在接收到SIGINT时被调用
        def signal_handler(signal, frame):
            self.cancelAllJobs()#取消已安排或正在运行的所有作业
            raise KeyboardInterrupt()#抛出用户中断执行的异常

        # see http://stackoverflow.com/questions/23206787/
        if isinstance(threading.current_thread(), threading._MainThread):
            signal.signal(signal.SIGINT, signal_handler)#预设信号处理函数singnal.signal(signalnum, handler)其中第一个参数是信号量，第二个参数信号处理函数。

    def __repr__(self):
		#返回格式化后的master和appName属性
        return "<SparkContext master={master} appName={appName}>".format(
            master=self.master,
            appName=self.appName,
        )

    def _repr_html_(self):
		#返回格式为html页面的SparkConf信息
        return """
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="{sc.uiWebUrl}">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v{sc.version}</code></dd>
              <dt>Master</dt>
                <dd><code>{sc.master}</code></dd>
              <dt>AppName</dt>
                <dd><code>{sc.appName}</code></dd>
            </dl>
        </div>
        """.format(
            sc=self
        )

    def _initialize_context(self, jconf):
        """
        Initialize SparkContext in function to allow subclass specific initialization
		在函数中初始化SparkContext以允许子类特定的初始化
        """
        return self._jvm.JavaSparkContext(jconf)

    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None, conf=None):
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
		检查SparkContext是否被初始化，如果SparkContext已经在运行，则会引发异常
		参考：_active_spark_context=None
        """
        with SparkContext._lock:
            if not SparkContext._gateway:
                SparkContext._gateway = gateway or launch_gateway(conf)
                SparkContext._jvm = SparkContext._gateway.jvm

            if instance:
                if (SparkContext._active_spark_context and
                        SparkContext._active_spark_context != instance):
                    currentMaster = SparkContext._active_spark_context.master
                    currentAppName = SparkContext._active_spark_context.appName
                    callsite = SparkContext._active_spark_context._callsite

                    # Raise error if there is already a running Spark context
					#如果SparkContext已经在运行，则会引发异常
                    raise ValueError(
                        "Cannot run multiple SparkContexts at once; "
                        "existing SparkContext(app=%s, master=%s)"
                        " created by %s at %s:%s "
                        % (currentAppName, currentMaster,
                            callsite.function, callsite.file, callsite.linenum))
                else:
                    SparkContext._active_spark_context = instance

    def __getnewargs__(self):
        # This method is called when attempting to pickle SparkContext, which is always an error:
		#该方法在试图序列化SparkContext时被调用，它通常是一个error
        raise Exception(
            "It appears that you are attempting to reference SparkContext from a broadcast "
            "variable, action, or transformation. SparkContext can only be used on the driver, "
            "not in code that it run on workers. For more information, see SPARK-5063."
        )

    def __enter__(self):
        """
        Enable 'with SparkContext(...) as sc: app(sc)' syntax.
		允许使用
		"with SparkContext(...) as sc:
			sc:app(sc)
		"
		的语法
        """
        return self

    def __exit__(self, type, value, trace):
        """
        Enable 'with SparkContext(...) as sc: app' syntax.

        Specifically stop the context on exit of the with block.
        """
        self.stop()

    @classmethod
    def getOrCreate(cls, conf=None):
        """
        Get or instantiate a SparkContext and register it as a singleton object.
		获取或实例化一个SparkContext并将其注册为一个单例对象。

        :param conf: SparkConf (optional)
        """
        with SparkContext._lock:
            if SparkContext._active_spark_context is None:
                SparkContext(conf=conf or SparkConf())
            return SparkContext._active_spark_context

    def setLogLevel(self, logLevel):
        """
        Control our logLevel. This overrides any user-defined log settings.
        Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
		控制log级别。这将覆盖任何自定义的日志设置。有效的日志级别包括:ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
        """
        self._jsc.setLogLevel(logLevel)

    @classmethod
    def setSystemProperty(cls, key, value):
        """
        Set a Java system property, such as spark.executor.memory. This must
        must be invoked before instantiating SparkContext.
		设置一个Java系统属性，比如spark.executor.memory。这必须必须在实例化SparkContext之前调用。
        """
        SparkContext._ensure_initialized()
        SparkContext._jvm.java.lang.System.setProperty(key, value)

    @property
    def version(self):
        """
        The version of Spark on which this application is running.
		这个应用程序正在运行的Spark版本
        """
        return self._jsc.version()

    @property
    @ignore_unicode_prefix
    def applicationId(self):
        """
        A unique identifier for the Spark application.
        Its format depends on the scheduler implementation.
		Spark应用程序的唯一标识符，它的格式取决于调度程序的实现。

        * in case of local spark app something like 'local-1433865536131'
        * in case of YARN something like 'application_1433865536131_34483'

        >>> sc.applicationId  # doctest: +ELLIPSIS
        u'local-...'
        """
        return self._jsc.sc().applicationId()

    @property
    def uiWebUrl(self):
        """Return the URL of the SparkUI instance started by this SparkContext"""
		#返回由这个SparkContext启动的SparkUI实例的URL
        return self._jsc.sc().uiWebUrl().get()

    @property
    def startTime(self):
        """Return the epoch time when the Spark Context was started."""
		#返回Spark上下文启动时的时间
        return self._jsc.startTime()

    @property
    def defaultParallelism(self):
        """
        Default level of parallelism to use when not given by user (e.g. for
        reduce tasks)
		在用户没有指定的情况下默认使用的并行度(例如分解任务)
        """
        return self._jsc.sc().defaultParallelism()

    @property
    def defaultMinPartitions(self):
        """
        Default min number of partitions for Hadoop RDDs when not given by user
		在用户没有指定的情况下，Hadoop RDDs的默认分区数量
        """
        return self._jsc.sc().defaultMinPartitions()

    def stop(self):
        """
        Shut down the SparkContext.
		关闭SparkContext。
        """
        if getattr(self, "_jsc", None):
            try:
                self._jsc.stop()
            except Py4JError:
                # Case: SPARK-18523
                warnings.warn(
                    'Unable to cleanly shutdown Spark JVM process.'
                    ' It is possible that the process has crashed,'
                    ' been killed or may also be in a zombie state.',
                    RuntimeWarning
                )
                pass
            finally:
                self._jsc = None
        if getattr(self, "_accumulatorServer", None):
            self._accumulatorServer.shutdown()
            self._accumulatorServer = None
        with SparkContext._lock:
            SparkContext._active_spark_context = None

    def emptyRDD(self):
        """
        Create an RDD that has no partitions or elements.
		创建一个没有分区或元素的RDD。
        """
        return RDD(self._jsc.emptyRDD(), self, NoOpSerializer())

    def range(self, start, end=None, step=1, numSlices=None):
        """
        Create a new RDD of int containing elements from `start` to `end`
        (exclusive), increased by `step` every element. Can be called the same
        way as python's built-in range() function. If called with a single argument,
        the argument is interpreted as `end`, and `start` is set to 0.
		创建一个从start到end，步长为step所构成的RDD,和python的range()方法类似。
		如果只给定一个参数，则默认是end的值，此时start则设置为0

        :param start: the start value
        :param end: the end value (exclusive)
        :param step: the incremental step (default: 1)
        :param numSlices: the number of partitions of the new RDD新RDD的分区数量
        :return: An RDD of int

        >>> sc.range(5).collect()
        [0, 1, 2, 3, 4]
        >>> sc.range(2, 4).collect()
        [2, 3]
        >>> sc.range(1, 7, 2).collect()
        [1, 3, 5]
        """
        if end is None:
            end = start
            start = 0

        return self.parallelize(xrange(start, end, step), numSlices)

    def parallelize(self, c, numSlices=None):
        """
        Distribute a local Python collection to form an RDD. Using xrange
        is recommended if the input represents a range for performance.
		分发一个本地的Python集合以形成一个RDD。如果输入表示的是一个范围推荐使用xrange。

        >>> sc.parallelize([0, 2, 3, 4, 6], 5).glom().collect()
        [[0], [2], [3], [4], [6]]
        >>> sc.parallelize(xrange(0, 6, 2), 5).glom().collect()
        [[], [0], [], [2], [4]]
        """
        numSlices = int(numSlices) if numSlices is not None else self.defaultParallelism
        if isinstance(c, xrange):
            size = len(c)
            if size == 0:
                return self.parallelize([], numSlices)
            step = c[1] - c[0] if size > 1 else 1
            start0 = c[0]

            def getStart(split):
                return start0 + int((split * size / numSlices)) * step

            def f(split, iterator):
                return xrange(getStart(split), getStart(split + 1), step)

            return self.parallelize([], numSlices).mapPartitionsWithIndex(f)
        # Calling the Java parallelize() method with an ArrayList is too slow,
        # because it sends O(n) Py4J commands.  As an alternative, serialized
        # objects are written to a file and loaded through textFile().
		#对一个ArrayList调用Java的并行化方法速度太慢，因为它要发送O(n)Py4J命令。作为替代,序列化对象被写入到一个文件中，并通过textFile()加载。
        tempFile = NamedTemporaryFile(delete=False, dir=self._temp_dir)
        try:
            # Make sure we distribute data evenly if it's smaller than self.batchSize
			#确保我们均匀地分布数据如果它小于self.batchsize
            if "__len__" not in dir(c):
                c = list(c)    # Make it a list so we can compute its length将其转换成list,以便可以计算它的长度
            batchSize = max(1, min(len(c) // numSlices, self._batchSize or 1024))#计算块大小
            serializer = BatchedSerializer(self._unbatched_serializer, batchSize)#获取分块后的序列化对象
            serializer.dump_stream(c, tempFile)#写入临时文件中
            tempFile.close()#关闭临时文件
            readRDDFromFile = self._jvm.PythonRDD.readRDDFromFile
            jrdd = readRDDFromFile(self._jsc, tempFile.name, numSlices)
        finally:
            # readRDDFromFile eagerily reads the file so we can delete right after.
			#readRDDFromFile将快速读取文件，以便我们可以在之后删除
            os.unlink(tempFile.name)#删除临时文件,如果文件是一个目录则返回一个错误
        return RDD(jrdd, self, serializer)

    def pickleFile(self, name, minPartitions=None):
        """
        Load an RDD previously saved using L{RDD.saveAsPickleFile} method.
		使用{RDD.saveAsPickleFile}保存一个以前的RDD

        >>> tmpFile = NamedTemporaryFile(delete=True)
        >>> tmpFile.close()
        >>> sc.parallelize(range(10)).saveAsPickleFile(tmpFile.name, 5)
        >>> sorted(sc.pickleFile(tmpFile.name, 3).collect())
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        """
        minPartitions = minPartitions or self.defaultMinPartitions#获取最小分区数
        return RDD(self._jsc.objectFile(name, minPartitions), self)

    @ignore_unicode_prefix
    def textFile(self, name, minPartitions=None, use_unicode=True):
        """
        Read a text file from HDFS, a local file system (available on all
        nodes), or any Hadoop-supported file system URI, and return it as an
        RDD of Strings.
		从HDFS、或者本地文件系统（所有节点可用）、或任何hadoop支持的文件系统URI，读取一个文本文件，并将其作为一个字符串的RDD返回。

        If use_unicode is False, the strings will be kept as `str` (encoding
        as `utf-8`), which is faster and smaller than unicode. (Added in
        Spark 1.2)
		如果use_unicode设置为False，则字符串将保持为‘str’(encoding as `utf-8`)类型，它比unicode更快、更小。(spark1.2添加的)

        >>> path = os.path.join(tempdir, "sample-text.txt")
        >>> with open(path, "w") as testFile:
        ...    _ = testFile.write("Hello world!")
        >>> textFile = sc.textFile(path)
        >>> textFile.collect()
        [u'Hello world!']
        """
        minPartitions = minPartitions or min(self.defaultParallelism, 2)#获取最小分区
        return RDD(self._jsc.textFile(name, minPartitions), self,
                   UTF8Deserializer(use_unicode))

    @ignore_unicode_prefix
    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        """
        Read a directory of text files from HDFS, a local file system
        (available on all nodes), or any  Hadoop-supported file system
        URI. Each file is read as a single record and returned in a
        key-value pair, where the key is the path of each file, the
        value is the content of each file.
		从HDFS、或者本地文件系统（所有节点可用）、或任何hadoop支持的文件系统URI，读取一个文本文件的目录。
		每个文件都被读取为单个记录，并以键-值对返回，其中键是每个文件的路径， value是每个文件的内容。

        If use_unicode is False, the strings will be kept as `str` (encoding
        as `utf-8`), which is faster and smaller than unicode. (Added in
        Spark 1.2)
		如果use_unicode设置为False，则字符串将保持为‘str’(encoding as `utf-8`)类型，它比unicode更快、更小。(spark1.2添加的)

        For example, if you have the following files::

          hdfs://a-hdfs-path/part-00000
          hdfs://a-hdfs-path/part-00001
          ...
          hdfs://a-hdfs-path/part-nnnnn

        Do C{rdd = sparkContext.wholeTextFiles("hdfs://a-hdfs-path")},
        then C{rdd} contains::

          (a-hdfs-path/part-00000, its content)
          (a-hdfs-path/part-00001, its content)
          ...
          (a-hdfs-path/part-nnnnn, its content)

        .. note:: Small files are preferred, as each file will be loaded
            fully in memory.
				小文件优先，每个文件都会被全部加在到内存

        >>> dirPath = os.path.join(tempdir, "files")
        >>> os.mkdir(dirPath)
        >>> with open(os.path.join(dirPath, "1.txt"), "w") as file1:
        ...    _ = file1.write("1")
        >>> with open(os.path.join(dirPath, "2.txt"), "w") as file2:
        ...    _ = file2.write("2")
        >>> textFiles = sc.wholeTextFiles(dirPath)
        >>> sorted(textFiles.collect())
        [(u'.../1.txt', u'1'), (u'.../2.txt', u'2')]
        """
        minPartitions = minPartitions or self.defaultMinPartitions
        return RDD(self._jsc.wholeTextFiles(path, minPartitions), self,
                   PairDeserializer(UTF8Deserializer(use_unicode), UTF8Deserializer(use_unicode)))

    def binaryFiles(self, path, minPartitions=None):
        """
        .. note:: Experimental

        Read a directory of binary files from HDFS, a local file system
        (available on all nodes), or any Hadoop-supported file system URI
        as a byte array. Each file is read as a single record and returned
        in a key-value pair, where the key is the path of each file, the
        value is the content of each file.
		从HDFS、或者本地文件系统（所有节点可用）、或任何hadoop支持的文件系统URI，读取一个二进制文件的目录。
		每个文件都被读取为单个记录，并以键-值对返回，其中键是每个文件的路径， value是每个文件的内容。

        .. note:: Small files are preferred, large file is also allowable, but
            may cause bad performance.
        """
        minPartitions = minPartitions or self.defaultMinPartitions#获取最小的分区数
        return RDD(self._jsc.binaryFiles(path, minPartitions), self,
                   PairDeserializer(UTF8Deserializer(), NoOpSerializer()))

    def binaryRecords(self, path, recordLength):
        """
        .. note:: Experimental

        Load data from a flat binary file, assuming each record is a set of numbers
        with the specified numerical format (see ByteBuffer), and the number of
        bytes per record is constant.
		从平面二进制文件中加载数据，假设每个记录都是使用指定的数值格式的一个数据集，切每条记录的字节数是常量。

        :param path: Directory to the input data files输入文件的目录
        :param recordLength: The length at which to split the records分割记录的长度
        """
        return RDD(self._jsc.binaryRecords(path, recordLength), self, NoOpSerializer())

    def _dictToJavaMap(self, d):
		"""
		将一个字典dict转化为java map
		"""
        jm = self._jvm.java.util.HashMap()
        if not d:
            d = {}
        for k, v in d.items():
            jm[k] = v
        return jm

    def sequenceFile(self, path, keyClass=None, valueClass=None, keyConverter=None,
                     valueConverter=None, minSplits=None, batchSize=0):
        """
        Read a Hadoop SequenceFile with arbitrary key and value Writable class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is as follows:
		从HDFS、或者本地文件系统（所有节点可用）、或任何hadoop支持的文件系统URI，读取任意键和值可写类的Hadoop序列文件，其机制如下:

            1. A Java RDD is created from the SequenceFile or other InputFormat, and the key
               and value Writable classes
			   Java RDD是由序列文件或其他输入格式创建的，并且键和值是可写的类
            2. Serialization is attempted via Pyrolite pickling
				通过Pyrolite pickling进行序列化
            3. If this fails, the fallback is to call 'toString' on each key and value
				如果操作失败，则回调采用的是在每个键和值上调用'toString'
            4. C{PickleSerializer} is used to deserialize pickled objects on the Python side
				{PickleSerializer}用于在Python端对pickle对象进行反序列化

        :param path: path to sequncefile序列文件的路径
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter:
        :param valueConverter:
        :param minSplits: minimum splits in dataset
               (default min(2, sc.defaultParallelism))
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        minSplits = minSplits or min(self.defaultParallelism, 2)#获取分片个数（即：分区个数）
        jrdd = self._jvm.PythonRDD.sequenceFile(self._jsc, path, keyClass, valueClass,
                                                keyConverter, valueConverter, minSplits, batchSize)
        return RDD(jrdd, self)

    def newAPIHadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None,
                         valueConverter=None, conf=None, batchSize=0):
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for sc.sequenceFile.

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java

        :param path: path to Hadoop file
        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.newAPIHadoopFile(self._jsc, path, inputFormatClass, keyClass,
                                                    valueClass, keyConverter, valueConverter,
                                                    jconf, batchSize)
        return RDD(jrdd, self)

    def newAPIHadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None,
                        valueConverter=None, conf=None, batchSize=0):
        """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for sc.sequenceFile.

        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.newAPIHadoopRDD(self._jsc, inputFormatClass, keyClass,
                                                   valueClass, keyConverter, valueConverter,
                                                   jconf, batchSize)
        return RDD(jrdd, self)

    def hadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None,
                   valueConverter=None, conf=None, batchSize=0):
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for sc.sequenceFile.

        A Hadoop configuration can be passed in as a Python dict. This will be converted into a
        Configuration in Java.

        :param path: path to Hadoop file
        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapred.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.hadoopFile(self._jsc, path, inputFormatClass, keyClass,
                                              valueClass, keyConverter, valueConverter,
                                              jconf, batchSize)
        return RDD(jrdd, self)

    def hadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None,
                  valueConverter=None, conf=None, batchSize=0):
        """
        Read an 'old' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        Hadoop configuration, which is passed in as a Python dict.
        This will be converted into a Configuration in Java.
        The mechanism is the same as for sc.sequenceFile.

        :param inputFormatClass: fully qualified classname of Hadoop InputFormat
               (e.g. "org.apache.hadoop.mapred.TextInputFormat")
        :param keyClass: fully qualified classname of key Writable class
               (e.g. "org.apache.hadoop.io.Text")
        :param valueClass: fully qualified classname of value Writable class
               (e.g. "org.apache.hadoop.io.LongWritable")
        :param keyConverter: (None by default)
        :param valueConverter: (None by default)
        :param conf: Hadoop configuration, passed in as a dict
               (None by default)
        :param batchSize: The number of Python objects represented as a single
               Java object. (default 0, choose batchSize automatically)
        """
        jconf = self._dictToJavaMap(conf)
        jrdd = self._jvm.PythonRDD.hadoopRDD(self._jsc, inputFormatClass, keyClass,
                                             valueClass, keyConverter, valueConverter,
                                             jconf, batchSize)
        return RDD(jrdd, self)

    def _checkpointFile(self, name, input_deserializer):
        jrdd = self._jsc.checkpointFile(name)
        return RDD(jrdd, self, input_deserializer)

    @ignore_unicode_prefix
    def union(self, rdds):
        """
        Build the union of a list of RDDs.
		构建一个RDDs列表的联合。

        This supports unions() of RDDs with different serialized formats,
        although this forces them to be reserialized using the default
        serializer:

        >>> path = os.path.join(tempdir, "union-text.txt")
        >>> with open(path, "w") as testFile:
        ...    _ = testFile.write("Hello")
        >>> textFile = sc.textFile(path)
        >>> textFile.collect()
        [u'Hello']
        >>> parallelized = sc.parallelize(["World!"])
        >>> sorted(sc.union([textFile, parallelized]).collect())
        [u'Hello', 'World!']
        """
        first_jrdd_deserializer = rdds[0]._jrdd_deserializer
        if any(x._jrdd_deserializer != first_jrdd_deserializer for x in rdds):
            rdds = [x._reserialize() for x in rdds]
        first = rdds[0]._jrdd
        rest = [x._jrdd for x in rdds[1:]]
        return RDD(self._jsc.union(first, rest), self, rdds[0]._jrdd_deserializer)

    def broadcast(self, value):
        """
        Broadcast a read-only variable to the cluster, returning a
        L{Broadcast<pyspark.broadcast.Broadcast>}
        object for reading it in distributed functions. The variable will
        be sent to each cluster only once.
		向集群广播一个只读变量，返回一个用于在分布式函数中读取它的{Broadcast<pyspark.broadcast.Broadcast>}对象。变量会只被发送到每个集群一次。
        """
        return Broadcast(self, value, self._pickled_broadcast_vars)

    def accumulator(self, value, accum_param=None):
        """
        Create an L{Accumulator} with the given initial value, using a given
        L{AccumulatorParam} helper object to define how to add values of the
        data type if provided. Default AccumulatorParams are used for integers
        and floating-point numbers if you do not provide one. For other types,
        a custom AccumulatorParam can be used.
		通过给定的初始值创建一个累加器{Accumulator}，通过给定的{AccumulatorParam}来帮助对象定义如何添加指定类型的值。
		如果不指定类型，默认的累计器参数是整型和float型。对于其他类型, 可以使用自定义的累计器参数。
        """
		
        if accum_param is None:
            if isinstance(value, int):#整型
                accum_param = accumulators.INT_ACCUMULATOR_PARAM
            elif isinstance(value, float):#float
                accum_param = accumulators.FLOAT_ACCUMULATOR_PARAM
            elif isinstance(value, complex):#复数
                accum_param = accumulators.COMPLEX_ACCUMULATOR_PARAM
            else:
                raise TypeError("No default accumulator param for type %s" % type(value))
        SparkContext._next_accum_id += 1
        return Accumulator(SparkContext._next_accum_id - 1, value, accum_param)

    def addFile(self, path, recursive=False):
        """
        Add a file to be downloaded with this Spark job on every node.
        The C{path} passed can be either a local file, a file in HDFS
        (or other Hadoop-supported filesystems), or an HTTP, HTTPS or
        FTP URI.
		添加在这个Spark job中每个节点上都需要下载的文件。
        {path}可以是本地文件，也可以是HDFS中的文件（或其他Hadoop支持的文件系统），或HTTP，HTTPS或FTP URI。

        To access the file in Spark jobs, use
        L{SparkFiles.get(fileName)<pyspark.files.SparkFiles.get>} with the
        filename to find its download location.
		要访问Spark作业中的文件，需要使用文件名和{SparkFiles.get（fileName）<pyspark.files.SparkFiles.get>}来找到它的下载位置。

        A directory can be given if the recursive option is set to True.
        Currently directories are only supported for Hadoop-supported filesystems.
		如果递归选项recursive设置为True，则可以给出一个目录。
        当前目录仅支持Hadoop所支持的文件系统。
		
        >>> from pyspark import SparkFiles
        >>> path = os.path.join(tempdir, "test.txt")
        >>> with open(path, "w") as testFile:
        ...    _ = testFile.write("100")
        >>> sc.addFile(path)
        >>> def func(iterator):
        ...    with open(SparkFiles.get("test.txt")) as testFile:
        ...        fileVal = int(testFile.readline())
        ...        return [x * fileVal for x in iterator]
        >>> sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()
        [100, 200, 300, 400]
        """
        self._jsc.sc().addFile(path, recursive)

    def addPyFile(self, path):
        """
        Add a .py or .zip dependency for all tasks to be executed on this
        SparkContext in the future.  The C{path} passed can be either a local
        file, a file in HDFS (or other Hadoop-supported filesystems), or an
        HTTP, HTTPS or FTP URI.
		为以后在这个SparkContext上执行的所有任务添加需要的.py或.zip依赖项。
		{path}可以是本地的文件，HDFS（或其他Hadoop支持的文件系统）中的文件，或者HTTP，HTTPS或FTP URI。
        """
        self.addFile(path)
        (dirname, filename) = os.path.split(path)  # dirname may be directory or HDFS/S3 prefix 目录名可能是目录或hdfs/s3前缀
        if filename[-4:].lower() in self.PACKAGE_EXTENSIONS:#如果文件名的后缀在('.zip', '.egg', '.jar')中则将该文件添加到路径中去
            self._python_includes.append(filename)
            # for tests in local mode本地测试
            sys.path.insert(1, os.path.join(SparkFiles.getRootDirectory(), filename))
        if sys.version > '3':
            import importlib
            importlib.invalidate_caches()

    def setCheckpointDir(self, dirName):
        """
        Set the directory under which RDDs are going to be checkpointed. The
        directory must be a HDFS path if running on a cluster.
		设置RDDs将被检测的目录。如果在集群上运行，目录则必须是HDFS路径。
        """
        self._jsc.sc().setCheckpointDir(dirName)

    def _getJavaStorageLevel(self, storageLevel):
        """
        Returns a Java StorageLevel based on a pyspark.StorageLevel.
		返回一个基于pyspark.StorageLevel的Java存储级别StorageLevel。
        """
        if not isinstance(storageLevel, StorageLevel):
            raise Exception("storageLevel must be of type pyspark.StorageLevel")

        newStorageLevel = self._jvm.org.apache.spark.storage.StorageLevel
        return newStorageLevel(storageLevel.useDisk,
                               storageLevel.useMemory,
                               storageLevel.useOffHeap,
                               storageLevel.deserialized,
                               storageLevel.replication)

    def setJobGroup(self, groupId, description, interruptOnCancel=False):
        """
        Assigns a group ID to all the jobs started by this thread until the group ID is set to a
        different value or cleared.
		分配一个组ID给由该线程启动的所有作业，直到组ID被设置为其他值或被清除了。

        Often, a unit of execution in an application consists of multiple Spark actions or jobs.
        Application programmers can use this method to group all those jobs together and give a
        group description. Once set, the Spark web UI will associate such jobs with this group.
		通常，应用程序中的一个执行单元由多个Spark action或jobs组成。
		应用程序程序员可以使用这种方法将所有这些工作组合在一起，并提供一个组描述。一旦设置完成，Spark web UI将把此类作业和这个组关联起来。
		

        The application can use L{SparkContext.cancelJobGroup} to cancel all
        running jobs in this group.
		应用程序可以使用{SparkContext.cancelJobGroup}取消这个组的所有工作

        >>> import threading
        >>> from time import sleep
        >>> result = "Not Set"
        >>> lock = threading.Lock()
        >>> def map_func(x):
        ...     sleep(100)
        ...     raise Exception("Task should have been cancelled")
        >>> def start_job(x):
        ...     global result
        ...     try:
        ...         sc.setJobGroup("job_to_cancel", "some description")
        ...         result = sc.parallelize(range(x)).map(map_func).collect()
        ...     except Exception as e:
        ...         result = "Cancelled"
        ...     lock.release()
        >>> def stop_job():
        ...     sleep(5)
        ...     sc.cancelJobGroup("job_to_cancel")
        >>> supress = lock.acquire()
        >>> supress = threading.Thread(target=start_job, args=(10,)).start()
        >>> supress = threading.Thread(target=stop_job).start()
        >>> supress = lock.acquire()
        >>> print(result)
        Cancelled

        If interruptOnCancel is set to true for the job group, then job cancellation will result
        in Thread.interrupt() being called on the job's executor threads. This is useful to help
        ensure that the tasks are actually stopped in a timely manner, but is off by default due
        to HDFS-1208, where HDFS may respond to Thread.interrupt() by marking nodes as dead.
		如果job组的interruptOnCancel被设置为true，那么作业的取消将会导致Thread.interrupt()在job的执行线程中被调用。
		这在确保任务实际上是及时停止的非常有用，但是在HDFS-1208中默认情况下是关闭的，也许是因为通过标记一个节点为“dead”状态来响应Thread.interrupt()
        """
        self._jsc.setJobGroup(groupId, description, interruptOnCancel)

    def setLocalProperty(self, key, value):
        """
        Set a local property that affects jobs submitted from this thread, such as the
        Spark fair scheduler pool.
		设置一个本地属性，该属性将影响从该线程提交的作业，例如spark公平调度器池。
        """
        self._jsc.setLocalProperty(key, value)

    def getLocalProperty(self, key):
        """
        Get a local property set in this thread, or null if it is missing. See
        L{setLocalProperty}
		在该线程中获得一个本地属性集，如果缺少则为null。
        """
        return self._jsc.getLocalProperty(key)

    def sparkUser(self):
        """
        Get SPARK_USER for user who is running SparkContext.
		获取运行SparkContext的用户。
        """
        return self._jsc.sc().sparkUser()

    def cancelJobGroup(self, groupId):
        """
        Cancel active jobs for the specified group. See L{SparkContext.setJobGroup}
        for more information.
		取消指定组的正在工作的作业。
        """
        self._jsc.sc().cancelJobGroup(groupId)

    def cancelAllJobs(self):
        """
        Cancel all jobs that have been scheduled or are running.
		取消已安排或正在运行的所有作业。
        """
        self._jsc.sc().cancelAllJobs()

    def statusTracker(self):
        """
        Return :class:`StatusTracker` object
        """
        return StatusTracker(self._jsc.statusTracker())

    def runJob(self, rdd, partitionFunc, partitions=None, allowLocal=False):
        """
        Executes the given partitionFunc on the specified set of partitions,
        returning the result as an array of elements.
		在指定的分区上执行给定的分区函数，返回一个数组作为结果

        If 'partitions' is not specified, this will run over all partitions.
		如果没有指定“分区”，则会在所有分区上运行。

        >>> myRDD = sc.parallelize(range(6), 3)
        >>> sc.runJob(myRDD, lambda part: [x * x for x in part])
        [0, 1, 4, 9, 16, 25]

        >>> myRDD = sc.parallelize(range(6), 3)
        >>> sc.runJob(myRDD, lambda part: [x * x for x in part], [0, 2], True)
        [0, 1, 16, 25]
        """
        if partitions is None:
            partitions = range(rdd._jrdd.partitions().size())

        # Implementation note: This is implemented as a mapPartitions followed
        # by runJob() in order to avoid having to pass a Python lambda into
        # SparkContext#runJob.
        mappedRDD = rdd.mapPartitions(partitionFunc)
        port = self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, partitions)
        return list(_load_from_socket(port, mappedRDD._jrdd_deserializer))

    def show_profiles(self):
        """ Print the profile stats to stdout """
		#将配置属性打印到标准输出中stdout
        if self.profiler_collector is not None:
            self.profiler_collector.show_profiles()
        else:
            raise RuntimeError("'spark.python.profile' configuration must be set "
                               "to 'true' to enable Python profile.")

    def dump_profiles(self, path):
        """ Dump the profile stats into directory `path`
		将配置属性转储到`path`路径的目录中
        """
        if self.profiler_collector is not None:
            self.profiler_collector.dump_profiles(path)
        else:
            raise RuntimeError("'spark.python.profile' configuration must be set "
                               "to 'true' to enable Python profile.")

    def getConf(self):
        conf = SparkConf()
        conf.setAll(self._conf.getAll())
        return conf


def _test():
    import atexit
    import doctest
    import tempfile
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest')
    globs['tempdir'] = tempfile.mkdtemp()
    atexit.register(lambda: shutil.rmtree(globs['tempdir']))
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
