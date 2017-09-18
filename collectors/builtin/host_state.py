#!/usr/bin/python
import time
import psutil

from collectors.lib.collectorbase import CollectorBase
from collectors.lib import utils

class HostState(CollectorBase):

    def __init__(self, config, logger, readq):
        super(HostState, self).__init__(config, logger, readq)

    def __call__(self):
        pre_processes = self.get_processes()
        # wait 1s to get the rate of disk IO read & write
        time.sleep(1)
        cur_processes = self.get_processes()
        processes = self.cal_disk_io_rate(pre_processes, cur_processes)
        host_state = self.get_host_state(processes)
        try:

            utils.alertd_post_sender("/host/state", host_state)
            self._readq.nput("host state send %s %s" % (int(time.time()), '0'))

        except Exception as e:

            self.log_error("can't send host state result to alertd %s" % e)
            self._readq.nput("host state send %s %s" % (int(time.time()), '1'))


    def cal_disk_io_rate(self, pre_processes, cur_processes):

        processes = []
        for pid in cur_processes:
            cur_proc = cur_processes[pid]
            pre_proc = pre_processes[pid]
            if pre_proc is not None:
                cur_proc['diskIoRead'] = cur_proc['diskIoRead'] - pre_proc['diskIoRead']
                cur_proc['diskIoWrite'] = cur_proc['diskIoWrite'] - pre_proc['diskIoWrite']
                processes.append(cur_proc)
            else:
                print pid
                cur_proc['diskIoRead'] = 'N/A'
                cur_proc['diskIoWrite'] = 'N/A'
        return processes

    def get_host_state(self, processes):

        host_state = {}
        # in macos utils.get_ip doesn't work
        # host_state['ip'] = '192.168.1.177'
        host_state['ip'] = utils.get_ip(self._logger)
        host_state['host'] = utils.get_hostname(self._logger)
        host_state['key'] = host_state['ip'] + '_' + host_state['host']
        # host_state['key'] = '192.168.1.177_cent0'
        host_state['processes'] = processes

        print (host_state)
        return host_state

    def get_processes(self):

        processes = {}
        for pid in psutil.pids():
            try:
                process = {}
                proc = psutil.Process(pid)
                process['pid'] = pid
                process['name'] = proc.name()
                process['command'] = proc.exe()
                process['user'] = proc.username()
                process['memPercent'] = str(round(proc.memory_percent(), 2)) + '%'
                process['cpuPercent'] = str(round(proc.cpu_percent(), 2)) + '%'

                # in macos it doesn't work. in win and linux is work
                process['disk_io_read'] = proc.io_counters().read_chars
                process['disk_io_write'] = proc.io_counters().write_chars
                # process['diskIoRead'] = 2311
                # process['diskIoWrite'] = 2131

            except psutil.AccessDenied:
                continue
            except psutil.NoSuchProcess:
                continue
            processes[pid] = process
        return processes

