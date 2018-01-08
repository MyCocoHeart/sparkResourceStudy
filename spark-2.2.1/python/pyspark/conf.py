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

"""
>>> from pyspark.conf import SparkConf
>>> from pyspark.context import SparkContext
>>> conf = SparkConf()
>>> conf.setMaster("local").setAppName("My app")
<pyspark.conf.SparkConf object at ...>
>>> conf.get("spark.master")
u'local'
>>> conf.get("spark.app.name")
u'My app'
>>> sc = SparkContext(conf=conf)
>>> sc.master
u'local'
>>> sc.appName
u'My app'
>>> sc.sparkHome is None
True

>>> conf = SparkConf(loadDefaults=False)
>>> conf.setSparkHome("/path")
<pyspark.conf.SparkConf object at ...>
>>> conf.get("spark.home")
u'/path'
>>> conf.setExecutorEnv("VAR1", "value1")
<pyspark.conf.SparkConf object at ...>
>>> conf.setExecutorEnv(pairs = [("VAR3", "value3"), ("VAR4", "value4")])
<pyspark.conf.SparkConf object at ...>
>>> conf.get("spark.executorEnv.VAR1")
u'value1'
>>> print(conf.toDebugString())
spark.executorEnv.VAR1=value1
spark.executorEnv.VAR3=value3
spark.executorEnv.VAR4=value4
spark.home=/path
>>> sorted(conf.getAll(), key=lambda p: p[0])
[(u'spark.executorEnv.VAR1', u'value1'), (u'spark.executorEnv.VAR3', u'value3'), \
(u'spark.executorEnv.VAR4', u'value4'), (u'spark.home', u'/path')]
>>> conf._jconf.setExecutorEnv("VAR5", "value5")
JavaObject id...
>>> print(conf.toDebugString())
spark.executorEnv.VAR1=value1
spark.executorEnv.VAR3=value3
spark.executorEnv.VAR4=value4
spark.executorEnv.VAR5=value5
spark.home=/path
"""

__all__ = ['SparkConf']

import sys
import re

if sys.version > '3':
    unicode = str
    __doc__ = re.sub(r"(\W|^)[uU](['])", r'\1\2', __doc__)


class SparkConf(object):

    """
    Configuration for a Spark application. Used to set various Spark
    parameters as key-value pairs.
	Spark应用程序的配置。通过键值对来设置各种Spark参数。

    Most of the time, you would create a SparkConf object with
    C{SparkConf()}, which will load values from C{spark.*} Java system
    properties as well. In this case, any parameters you set directly on
    the C{SparkConf} object take priority over system properties.
	大多数情况下，您将通过{SparkConf()}创建一个SparkConf对象，它将加载{spark.*}和Java系统中的属性。
	在这种情况下，您直接在{SparkConf}对象中设置的任何参数都会优先于系统属性。


    For unit tests, you can also call C{SparkConf(false)} to skip
    loading external settings and get the same configuration no matter
    what the system properties are.
	作为单元测试，你也可以调用{SparkConf(false)}来跳过加载外部设置,并且无论系统属性是什么，你都获得相同的配置

    All setter methods in this class support chaining. For example,
    you can write C{conf.setMaster("local").setAppName("My app")}.
	 这个类中的所有setter方法都支持链接方式。比如可以使用：conf.setMaster("local").setAppName("My app")
	
    .. note:: Once a SparkConf object is passed to Spark, it is cloned
        and can no longer be modified by the user.
		一旦SparkConf对象被传递给Spark，它被克隆并且不能再由用户修改。
    """

    def __init__(self, loadDefaults=True, _jvm=None, _jconf=None):
        """
        Create a new Spark configuration.
		创建一个新的Spark配置

        :param loadDefaults: whether to load values from Java system
               properties (True by default)
			   是否从Java系统加载属性（默认为True）
        :param _jvm: internal parameter used to pass a handle to the
               Java VM; does not need to be set by users
			   用于传递一个操作到Java VM的内部参数; 不需要由用户设置
        :param _jconf: Optionally pass in an existing SparkConf handle
               to use its parameters
			   可以传入一个现有的SparkConf来使用其中的参数
        """
        if _jconf:
            self._jconf = _jconf
        else:
            from pyspark.context import SparkContext
            _jvm = _jvm or SparkContext._jvm

            if _jvm is not None:
                # JVM is created, so create self._jconf directly through JVM
				#JVM已经被创建了，所以直接通过JVM创建self._jconf
                self._jconf = _jvm.SparkConf(loadDefaults)
                self._conf = None
            else:
                # JVM is not created, so store data in self._conf first
				#JVM没有被创建则先把数据存储在self._conf中
                self._jconf = None
                self._conf = {}

    def set(self, key, value):
        """Set a configuration property."""
		#设置配置属性
        # Try to set self._jconf first if JVM is created, set self._conf if JVM is not created yet.
		#如果已经启动了JVM，则尝试先设置self._jconf，如果尚未启动JVM，则设置self._conf。
        if self._jconf is not None:
            self._jconf.set(key, unicode(value))
        else:
            self._conf[key] = unicode(value)
        return self

    def setIfMissing(self, key, value):
        """Set a configuration property, if not already set."""
		#如果尚未设置配置属性则设置配置属性
        if self.get(key) is None:
            self.set(key, value)
        return self

    def setMaster(self, value):
        """Set master URL to connect to."""
		#置要连接的主机URL
        self.set("spark.master", value)
        return self

    def setAppName(self, value):
        """Set application name."""
		#设置应用程序名称
        self.set("spark.app.name", value)
        return self

    def setSparkHome(self, value):
        """Set path where Spark is installed on worker nodes."""
		#设置工作站节点上安装的Spark路径。
        self.set("spark.home", value)
        return self

    def setExecutorEnv(self, key=None, value=None, pairs=None):
        """Set an environment variable to be passed to executors."""
		#设置一个需要传递给执行者的环境变量
        if (key is not None and pairs is not None) or (key is None and pairs is None):
            raise Exception("Either pass one key-value pair or a list of pairs")
        elif key is not None:
            self.set("spark.executorEnv." + key, value)
        elif pairs is not None:
            for (k, v) in pairs:
                self.set("spark.executorEnv." + k, v)
        return self

    def setAll(self, pairs):
        """
        Set multiple parameters, passed as a list of key-value pairs.
		设置多个参数，作为键值对列表传递

        :param pairs: list of key-value pairs to set
					要设置的键值对列表
        """
        for (k, v) in pairs:
            self.set(k, v)
        return self

    def get(self, key, defaultValue=None):
        """Get the configured value for some key, or return a default otherwise."""
		#获取某个键的配置值，否则返回默认值
        if defaultValue is None:   # Py4J doesn't call the right get() if we pass None
									#如果我们传递None，则Py4J不会调用正确的get()
            if self._jconf is not None:
                if not self._jconf.contains(key):
                    return None
                return self._jconf.get(key)
            else:
                if key not in self._conf:
                    return None
                return self._conf[key]
        else:
            if self._jconf is not None:
                return self._jconf.get(key, defaultValue)
            else:
                return self._conf.get(key, defaultValue)

    def getAll(self):
        """Get all values as a list of key-value pairs."""
		#获取所有值作为键值对列表
        if self._jconf is not None:
            return [(elem._1(), elem._2()) for elem in self._jconf.getAll()]
        else:
            return self._conf.items()

    def contains(self, key):
        """Does this configuration contain a given key?"""
		#查看配置中是否包含key属性？
        if self._jconf is not None:
            return self._jconf.contains(key)
        else:
            return key in self._conf

    def toDebugString(self):
        """
        Returns a printable version of the configuration, as a list of
        key=value pairs, one per line.
		以键值对构成的列表的形式返回可打印的配置版本，每行一个
        """
        if self._jconf is not None:
            return self._jconf.toDebugString()
        else:
            return '\n'.join('%s=%s' % (k, v) for k, v in self._conf.items())


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
