import time
from collectors.lib.collectorbase import CollectorBase
from collectors.lib.inc_processor import IncPorcessor

import socket

PORT_USED = '1'
PORT_UNUSED = '0'

class HostPortUsed(CollectorBase):

    def __init__(self, config, logger, readq):
        super(HostPortUsed, self).__init__(config, logger, readq)
        self.ip = self.get_config('ip')
        self.ports = self.get_config('port')

    def __call__(self):
        try:
            portList = self.ports.split(',')

            for port in portList:
                try:
                    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sk.settimeout(10)
                    sk.connect((self.ip, int(port)))
                    self._readq.nput("host.port.used %s %s port=%s" % (int(time.time()), PORT_USED, port))

                    sk.close()
                except Exception:
                    self._readq.nput("host.port.used %s %s port=%s" % (int(time.time()), PORT_UNUSED, port))

        except Exception as e:
            self.log_error("can't send host port usage result to alertd %s" % e)

