import time
import random
from collectors.lib.collectorbase import CollectorBase


class TestCollector(CollectorBase):

    def __init__(self, config, logger, readq):
        super(TestCollector, self).__init__(config, logger, readq)

    def __call__(self):
        ts = time.time()
        self._readq.nput('metric1 %d %d t1=10 t2=a' % (ts, random.randint(10, 1000)))
        self._readq.nput('metric2 %d %d t1=20 t2=b' % (ts, random.randint(100, 10000)))
        self._readq.nput('metric3 %d %d t1=30 t2=c' % (ts, random.randint(1, 1000)))
