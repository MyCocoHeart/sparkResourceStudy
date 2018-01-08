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

from collections import namedtuple
import os
import traceback


CallSite = namedtuple("CallSite", "function file linenum")


def first_spark_call():
    """
    Return a CallSite representing the first Spark call in the current call stack.
	返回一个表示当前调用堆栈中的第一个Spark调用的CallSite
	参考资料：traceback.extract_stack（f = None，limit = None ）
			  从当前堆栈帧中提取原始回溯。返回从回溯对象中提取的“预处理”堆栈跟踪条目列表。对于替代格式化的堆栈跟踪非常有用。
			  “预处理”堆栈跟踪条目是一个4元元组（filename, line number, function name, text），表示堆栈跟踪打印的信息。
			  text是去掉了头部和尾部空格的字符串; 如果资源不可用，则为None。
			  打印limit条堆栈跟踪条目（从调用点开始），如果limit为正数。否则，打印最后的abs(limit) 条目。
			  如果limit被忽略或者None则所有条目都被打印。可选的f参数用来指定堆栈帧开始位置
			  
			  sys.exc_info()
			  这个函数返回一个三元的元组，给出当前正在处理的异常的信息。
			  如果堆栈中的任何地方都没有异常处理，则返回一个包含三个None值的元组。
			  否则，返回的值为 (type, value, traceback)。则含义为：
					type获取正在处理的异常的类型（BaseException的一个子类）; 
					value获取异常实例（异常类型的一个实例）; 
					traceback得到一个回溯对象，它指向堆栈最初发生异常的地方。
    """
    tb = traceback.extract_stack()#获取异常条目，如果没有异常则返回None
    if len(tb) == 0:
        return None
    file, line, module, what = tb[len(tb) - 1]#堆栈中跟踪信息
    sparkpath = os.path.dirname(file)#返回file的目录
    first_spark_frame = len(tb) - 1
    for i in range(0, len(tb)):
        file, line, fun, what = tb[i]
        if file.startswith(sparkpath):
            first_spark_frame = i
            break
    if first_spark_frame == 0:
        file, line, fun, what = tb[0]
        return CallSite(function=fun, file=file, linenum=line)
    sfile, sline, sfun, swhat = tb[first_spark_frame]
    ufile, uline, ufun, uwhat = tb[first_spark_frame - 1]
    return CallSite(function=sfun, file=ufile, linenum=uline)


class SCCallSiteSync(object):
    """
    Helper for setting the spark context call site.

    Example usage:
    from pyspark.context import SCCallSiteSync
    with SCCallSiteSync(<relevant SparkContext>) as css:
        <a Spark call>
    """

    _spark_stack_depth = 0

    def __init__(self, sc):
        call_site = first_spark_call()
        if call_site is not None:
            self._call_site = "%s at %s:%s" % (
                call_site.function, call_site.file, call_site.linenum)
        else:
            self._call_site = "Error! Could not extract traceback info"
        self._context = sc

    def __enter__(self):
        if SCCallSiteSync._spark_stack_depth == 0:
            self._context._jsc.setCallSite(self._call_site)
        SCCallSiteSync._spark_stack_depth += 1

    def __exit__(self, type, value, tb):
        SCCallSiteSync._spark_stack_depth -= 1
        if SCCallSiteSync._spark_stack_depth == 0:
            self._context._jsc.setCallSite(None)
