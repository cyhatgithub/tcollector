# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

"""Base class for Checks.

If you are writing your own checks you should subclass the AgentCheck class.
The Check class is being deprecated so don't write new checks with it.
"""
# stdlib
from collections import defaultdict
import copy
import logging
import numbers
import os
import re
import time
import timeit
import traceback
from types import ListType, TupleType
import unicodedata

# 3p
try:
    import psutil
except ImportError:
    psutil = None
import yaml

# project


log = logging.getLogger(__name__)

# Default methods run when collecting info about the agent in developer mode
DEFAULT_PSUTIL_METHODS = ['memory_info', 'io_counters']

AGENT_METRICS_CHECK_NAME = 'agent_metrics'


# Konstants
class CheckException(Exception):
    pass


class Infinity(CheckException):
    pass


class NaN(CheckException):
    pass


class UnknownValue(CheckException):
    pass



#==============================================================================
# DEPRECATED
# ------------------------------
# If you are writing your own check, you should inherit from AgentCheck
# and not this class. This class will be removed in a future version
# of the agent.
#==============================================================================


class Check(object):
    """
    (Abstract) class for all checks with the ability to:
    * store 1 (and only 1) sample for gauges per metric/tag combination
    * compute rates for counters
    * only log error messages once (instead of each time they occur)

    """
    def __init__(self, logger):
        # where to store samples, indexed by metric_name
        # metric_name: {("sorted", "tags"): [(ts, value), (ts, value)],
        #                 tuple(tags) are stored as a key since lists are not hashable
        #               None: [(ts, value), (ts, value)]}
        #                 untagged values are indexed by None
        self._sample_store = {}
        self._counters = {}  # metric_name: bool
        self.logger = logger

    def normalize(self, metric, prefix=None):
        """Turn a metric into a well-formed metric name
        prefix.b.c
        """
        name = re.sub(r"[,\+\*\-/()\[\]{}\s]", "_", metric)
        # Eliminate multiple _
        name = re.sub(r"__+", "_", name)
        # Don't start/end with _
        name = re.sub(r"^_", "", name)
        name = re.sub(r"_$", "", name)
        # Drop ._ and _.
        name = re.sub(r"\._", ".", name)
        name = re.sub(r"_\.", ".", name)

        if prefix is not None:
            return prefix + "." + name
        else:
            return name

    def normalize_device_name(self, device_name):
        return device_name.strip().lower().replace(' ', '_')

    def counter(self, metric):
        """
        Treats the metric as a counter, i.e. computes its per second derivative
        ACHTUNG: Resets previous values associated with this metric.
        """
        self._counters[metric] = True
        self._sample_store[metric] = {}

    def is_counter(self, metric):
        "Is this metric a counter?"
        return metric in self._counters

    def gauge(self, metric):
        """
        Treats the metric as a gauge, i.e. keep the data as is
        ACHTUNG: Resets previous values associated with this metric.
        """
        self._sample_store[metric] = {}

    def is_metric(self, metric):
        return metric in self._sample_store

    def is_gauge(self, metric):
        return self.is_metric(metric) and \
            not self.is_counter(metric)

    def get_metric_names(self):
        "Get all metric names"
        return self._sample_store.keys()

    def save_gauge(self, metric, value, timestamp=None, tags=None, hostname=None, device_name=None):
        """ Save a gauge value. """
        if not self.is_gauge(metric):
            self.gauge(metric)
        self.save_sample(metric, value, timestamp, tags, hostname, device_name)

    def save_sample(self, metric, value, timestamp=None, tags=None, hostname=None, device_name=None):
        """Save a simple sample, evict old values if needed
        """
        from collectors.lib.checks.util import cast_metric_val

        if timestamp is None:
            timestamp = time.time()
        if metric not in self._sample_store:
            raise CheckException("Saving a sample for an undefined metric: %s" % metric)
        try:
            value = cast_metric_val(value)
        except ValueError as ve:
            raise NaN(ve)

        # Sort and validate tags
        if tags is not None:
            if type(tags) not in [type([]), type(())]:
                raise CheckException("Tags must be a list or tuple of strings")
            else:
                tags = tuple(sorted(tags))

        # Data eviction rules
        key = (tags, device_name)
        if self.is_gauge(metric):
            self._sample_store[metric][key] = ((timestamp, value, hostname, device_name), )
        elif self.is_counter(metric):
            if self._sample_store[metric].get(key) is None:
                self._sample_store[metric][key] = [(timestamp, value, hostname, device_name)]
            else:
                self._sample_store[metric][key] = self._sample_store[metric][key][-1:] + [(timestamp, value, hostname, device_name)]
        else:
            raise CheckException("%s must be either gauge or counter, skipping sample at %s" % (metric, time.ctime(timestamp)))

        if self.is_gauge(metric):
            # store[metric][tags] = (ts, val) - only 1 value allowed
            assert len(self._sample_store[metric][key]) == 1, self._sample_store[metric]
        elif self.is_counter(metric):
            assert len(self._sample_store[metric][key]) in (1, 2), self._sample_store[metric]

    @classmethod
    def _rate(cls, sample1, sample2):
        "Simple rate"
        try:
            interval = sample2[0] - sample1[0]
            if interval == 0:
                raise Infinity()

            delta = sample2[1] - sample1[1]
            if delta < 0:
                raise UnknownValue()

            return (sample2[0], delta / interval, sample2[2], sample2[3])
        except Infinity:
            raise
        except UnknownValue:
            raise
        except Exception as e:
            raise NaN(e)

    def get_sample_with_timestamp(self, metric, tags=None, device_name=None, expire=True):
        "Get (timestamp-epoch-style, value)"

        # Get the proper tags
        if tags is not None and isinstance(tags, ListType):
            tags.sort()
            tags = tuple(tags)
        key = (tags, device_name)

        # Never seen this metric
        if metric not in self._sample_store:
            raise UnknownValue()

        # Not enough value to compute rate
        elif self.is_counter(metric) and len(self._sample_store[metric][key]) < 2:
            raise UnknownValue()

        elif self.is_counter(metric) and len(self._sample_store[metric][key]) >= 2:
            res = self._rate(self._sample_store[metric][key][-2], self._sample_store[metric][key][-1])
            if expire:
                del self._sample_store[metric][key][:-1]
            return res

        elif self.is_gauge(metric) and len(self._sample_store[metric][key]) >= 1:
            return self._sample_store[metric][key][-1]

        else:
            raise UnknownValue()

    def get_sample(self, metric, tags=None, device_name=None, expire=True):
        "Return the last value for that metric"
        x = self.get_sample_with_timestamp(metric, tags, device_name, expire)
        assert isinstance(x, TupleType) and len(x) == 4, x
        return x[1]

    def get_samples_with_timestamps(self, expire=True):
        "Return all values {metric: (ts, value)} for non-tagged metrics"
        values = {}
        for m in self._sample_store:
            try:
                values[m] = self.get_sample_with_timestamp(m, expire=expire)
            except Exception:
                pass
        return values

    def get_samples(self, expire=True):
        "Return all values {metric: value} for non-tagged metrics"
        values = {}
        for m in self._sample_store:
            try:
                # Discard the timestamp
                values[m] = self.get_sample_with_timestamp(m, expire=expire)[1]
            except Exception:
                pass
        return values

    def get_metrics(self, expire=True):
        """Get all metrics, including the ones that are tagged.
        This is the preferred method to retrieve metrics

        @return the list of samples
        @rtype [(metric_name, timestamp, value, {"tags": ["tag1", "tag2"]}), ...]
        """
        metrics = []
        for m in self._sample_store:
            try:
                for key in self._sample_store[m]:
                    tags, device_name = key
                    try:
                        ts, val, hostname, device_name = self.get_sample_with_timestamp(m, tags, device_name, expire)
                    except UnknownValue:
                        continue
                    attributes = {}
                    if tags:
                        attributes['tags'] = list(tags)
                    if hostname:
                        attributes['host_name'] = hostname
                    if device_name:
                        attributes['device_name'] = device_name
                    metrics.append((m, int(ts), val, attributes))
            except Exception:
                pass
        return metrics
