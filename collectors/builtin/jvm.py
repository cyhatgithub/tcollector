import os
import re
import time

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

NO_DEADLOCK_THREAD = 0

JSTAT_GCUTIL_LIST = {
    'S0':'usage.survivor.space0',
    'S1':'usage.survivor.space1',
    'E':'usage.eden.space',
    'O':'usage.old.space',
    'P':'usage.perm.space',
    'YGC':'young.gc.frequency',
    'YGCT':'young.gc.time',
    'FGC':'full.gc.frequency',
    'FGCT':'full.gc.time',
    'GCT':'gc.total.time'
}

class Jvm(CollectorBase):

    def print_metric(self, metric, ts, value, tags=""):
        if value is not None:
            self._readq.nput("%s %d %s %s" % (metric, ts, value, tags))

    def __init__(self, config, logger, readq):
        super(Jvm, self).__init__(config, logger, readq)
        self.processes = self.get_config('processes')

    def __call__(self):
        # self.getDeadlockNum()
        # self.getJstat(89418, 'name')
        self.getJVM()
    def getJVM(self):
        pattern = re.compile(ur'(Found) [1-9]\d* (deadlock)')
        procList = self.processes.split(',')
        for proc in procList:
            pid = self.getPidFromProcName(proc)
            self.getDeadlockNum(pattern, pid, proc)
            self.getJstat(pid, proc)

    def getDeadlockNum(self, pattern, pid, proc):
        metric = 'jvm.deadlock.num'
        try:
            jstack = os.popen('jstack -l %s' % pid)
            logs = jstack.readlines()
            deadlock_num = NO_DEADLOCK_THREAD
            # from last to read because the deadlock log near the end
            for line in range(0, logs.__len__())[::-1]:
                str = logs[line]
                pattern_res = pattern.search(str)
                if pattern_res != None:
                    deadlock_num = self.getNumFromLog(str)
                    break
            tag = 'proccess=%s' % proc
            self.print_metric(metric, (int(time.time())), deadlock_num, tag)
            self._readq.nput("jvm.deadlock.num %s %s process=%s" % (int(time.time()), deadlock_num, proc))
        except Exception as e:
            self.log_error('failed to jstack -l proc:%s, %s' % (proc, e))


    def getNumFromLog(self, str):
        patternNum = re.compile(ur'[1-9]\d*')
        strList = str.split(' ')
        for str1 in strList:
            patterRes = patternNum.search(str1)
            if patterRes is not None:
                return str1
        return NO_DEADLOCK_THREAD


    def getPidFromProcName(self, name):
        jps = os.popen('jps | grep %s' % name)
        for line in jps.readlines():
            if line is not None:
                return line.split()[0]
        return None

    def getJstat(self, pid, proc):
        jstat = os.popen('jstat -gcutil %s' % pid)
        jList = []
        for line in jstat.readlines():
            if line is not None:
                jList.append(line.split())
        if jList is not None:
            keyVal = dict(zip(jList[0], jList[1]))
            for key in keyVal.keys():
                metric = 'jvm.jstat.gcutil.%s' % JSTAT_GCUTIL_LIST[key]
                tag = 'proccess=%s' % proc
                self.print_metric(metric, (int(time.time())), keyVal[key], tag)