#! /usr/local/bin/python2
import re
import time
import urllib
import urlparse
from collections import defaultdict

from collectors.lib.utils import remove_invalid_characters
from collectors.lib.collectorbase import CollectorBase

# 3p
import requests
from requests.exceptions import RequestException

EVENT_TYPE = SOURCE_TYPE_NAME = 'rabbitmq'
QUEUE_TYPE = 'queues'
NODE_TYPE = 'nodes'
CONNECTION_TYPE = 'connections'
MAX_DETAILED_QUEUES = 200
MAX_DETAILED_NODES = 100
# Post an event in the stream when the number of queues or nodes to
# collect is above 90% of the limit:
ALERT_THRESHOLD = 0.9
QUEUE_ATTRIBUTES = [
    # Path, Name, Operation
    ('active_consumers', 'active_consumers', float),
    ('consumers', 'consumers', float),
    ('consumer_utilisation', 'consumer_utilisation', float),

    ('memory', 'memory', float),

    ('messages', 'messages', float),
    ('messages_details/rate', 'messages.rate', float),

    ('messages_ready', 'messages_ready', float),
    ('messages_ready_details/rate', 'messages_ready.rate', float),

    ('messages_unacknowledged', 'messages_unacknowledged', float),
    ('messages_unacknowledged_details/rate', 'messages_unacknowledged.rate', float),

    ('message_stats/ack', 'messages.ack.count', float),
    ('message_stats/ack_details/rate', 'messages.ack.rate', float),

    ('message_stats/deliver', 'messages.deliver.count', float),
    ('message_stats/deliver_details/rate', 'messages.deliver.rate', float),

    ('message_stats/deliver_get', 'messages.deliver_get.count', float),
    ('message_stats/deliver_get_details/rate', 'messages.deliver_get.rate', float),

    ('message_stats/publish', 'messages.publish.count', float),
    ('message_stats/publish_details/rate', 'messages.publish.rate', float),

    ('message_stats/redeliver', 'messages.redeliver.count', float),
    ('message_stats/redeliver_details/rate', 'messages.redeliver.rate', float),
]

NODE_ATTRIBUTES = [
    ('fd_used', 'fd_used', float),
    ('mem_used', 'mem_used', float),
    ('run_queue', 'run_queue', float),
    ('sockets_used', 'sockets_used', float),
    ('partitions', 'partitions', len)
]

ATTRIBUTES = {
    QUEUE_TYPE: QUEUE_ATTRIBUTES,
    NODE_TYPE: NODE_ATTRIBUTES,
}

TAG_PREFIX = 'rabbitmq'
TAGS_MAP = {
    QUEUE_TYPE: {
        'node': 'node',
        'name': 'queue',
        'vhost': 'vhost',
        'policy': 'policy',
        'queue_family': 'queue_family',
    },
    NODE_TYPE: {
        'name': 'node',
    }
}

METRIC_SUFFIX = {
    QUEUE_TYPE: "queue",
    NODE_TYPE: "node",
}

CONFIG = {}


class RabbitMqException(Exception):
    pass


class RabbitMq(CollectorBase):
    def __init__(self, config, logger, readq):
        super(RabbitMq, self).__init__(config, logger, readq)
        try:
            CONFIG['rabbitmq_api_url'] = self.get_config('rabbitmq_api_url', 'http://192.168.1.105:15672/api/')
            CONFIG['rabbitmq_user'] = self.get_config('rabbitmq_user', 'guest')
            CONFIG['rabbitmq_pass'] = self.get_config('rabbitmq_pass', 'guest')
            CONFIG['ssl_verify'] = self.get_config('ssl_verify')
            CONFIG['tags'] = self.get_config('tags')
            CONFIG['queues_regexes'] = eval(self.get_config('queues_regexes', "['.*']"))
            CONFIG['nodes_regexes'] = eval(self.get_config('nodes_regexes', "['.*']"))
        except Exception as e:
            self.log_error("Illegal config!")

        if self.get_config('queues') != None:
            CONFIG['queues'] = list(str(self.get_config('queues')).split(','))
        if self.get_config('nodes') != None:
            CONFIG['nodes'] = list(str(self.get_config('nodes')).split(','))

    def __call__(self):
        self.check(CONFIG)

    def _get_config(self, instance):
        # make sure 'rabbitmq_api_url' is present and get parameters
        base_url = instance.get('rabbitmq_api_url', None)
        if not base_url:
            raise Exception('Missing "rabbitmq_api_url" in RabbitMQ config.')
        if not base_url.endswith('/'):
            base_url += '/'
        username = instance.get('rabbitmq_user', 'guest')
        password = instance.get('rabbitmq_pass', 'guest')
        custom_tags = ''
        parsed_url = urlparse.urlparse(base_url)
        ssl_verify = False
        if not ssl_verify and parsed_url.scheme == 'https':
            self.log_warn('Skipping SSL cert validation for %s based on configuration.' % base_url)

        # Limit of queues/nodes to collect metrics from
        max_detailed = {
            QUEUE_TYPE: int(instance.get('max_detailed_queues', MAX_DETAILED_QUEUES)),
            NODE_TYPE: int(instance.get('max_detailed_nodes', MAX_DETAILED_NODES)),
        }

        # List of queues/nodes to collect metrics from
        specified = {
            QUEUE_TYPE: {
                'explicit': instance.get('queues', []),
                'regexes': instance.get('queues_regexes', []),
            },
            NODE_TYPE: {
                'explicit': instance.get('nodes', []),
                'regexes': instance.get('nodes_regexes', []),
            },
        }

        for object_type, filters in specified.iteritems():
            for filter_type, filter_objects in filters.iteritems():
                if type(filter_objects) != list:
                    raise TypeError(
                        "{0} / {0}_regexes parameter must be a list".format(object_type))

        auth = (username, password)

        return base_url, max_detailed, specified, auth, ssl_verify, custom_tags

    def _get_vhosts(self, instance, base_url, auth=None, ssl_verify=True):
        vhosts = instance.get('vhosts')

        if not vhosts:
            # Fetch a list of _all_ vhosts from the API.
            vhosts_url = urlparse.urljoin(base_url, 'vhosts')
            # vhost_proxy = self.get_instance_proxy(instance, vhosts_url)
            vhosts_response = self._get_data(vhosts_url, auth=auth, ssl_verify=ssl_verify)
            vhosts = [v['name'] for v in vhosts_response]
        return vhosts

    def check(self, instance):
        base_url, max_detailed, specified, auth, ssl_verify, custom_tags = self._get_config(instance)
        try:
            # Generate metrics from the status API.
            self.get_stats(instance, base_url, QUEUE_TYPE, max_detailed[QUEUE_TYPE], specified[QUEUE_TYPE], custom_tags,
                           auth=auth, ssl_verify=ssl_verify)
            self.get_stats(instance, base_url, NODE_TYPE, max_detailed[NODE_TYPE], specified[NODE_TYPE], custom_tags,
                           auth=auth, ssl_verify=ssl_verify)
            vhosts = self._get_vhosts(instance, base_url, auth=auth, ssl_verify=ssl_verify)
            self.get_connections_stat(instance, base_url, CONNECTION_TYPE, vhosts, custom_tags,
                                      auth=auth, ssl_verify=ssl_verify)

            # Generate a service check from the aliveness API. In the case of an invalid response
            # code or unparseable JSON this check will send no data.
            self._check_aliveness(instance, base_url, vhosts, custom_tags, auth=auth, ssl_verify=ssl_verify)
            # Generate a service check for the service status.

        except RabbitMqException as e:
            msg = "Error executing check: {}".format(e)
            self.log_error(msg)

    def _get_data(self, url, auth=None, ssl_verify=True, proxies={}):
        try:
            r = requests.get(url, auth=auth, proxies=proxies, timeout=10, verify=ssl_verify)
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            raise RabbitMqException('Cannot open RabbitMQ API url: {} {}'.format(url, str(e)))
        except ValueError as e:
            raise RabbitMqException('Cannot parse JSON response from API url: {} {}'.format(url, str(e)))

    def get_stats(self, instance, base_url, object_type, max_detailed, filters, custom_tags, auth=None,
                  ssl_verify=True):
        """
        instance: the check instance
        base_url: the url of the rabbitmq management api (e.g. http://localhost:15672/api)
        object_type: either QUEUE_TYPE or NODE_TYPE
        max_detailed: the limit of objects to collect for this type
        filters: explicit or regexes filters of specified queues or nodes (specified in the yaml file)
        """
        data = self._get_data(urlparse.urljoin(base_url, object_type), auth=auth)

        # Make a copy of this list as we will remove items from it at each
        # iteration
        explicit_filters = filters['explicit']
        regex_filters = filters['regexes']

        """ data is a list of nodes or queues:
        data = [
            {'status': 'running', 'node': 'rabbit@host', 'name': 'queue1', 'consumers': 0, 'vhost': '/', 'backing_queue_status': {'q1': 0, 'q3': 0, 'q2': 0, 'q4': 0, 'avg_ack_egress_rate': 0.0, 'ram_msg_count': 0, 'ram_ack_count': 0, 'len': 0, 'persistent_count': 0, 'target_ram_count': 'infinity', 'next_seq_id': 0, 'delta': ['delta', 'undefined', 0, 'undefined'], 'pending_acks': 0, 'avg_ack_ingress_rate': 0.0, 'avg_egress_rate': 0.0, 'avg_ingress_rate': 0.0}, 'durable': True, 'idle_since': '2013-10-03 13:38:18', 'exclusive_consumer_tag': '', 'arguments': {}, 'memory': 10956, 'policy': '', 'auto_delete': False},
            {'status': 'running', 'node': 'rabbit@host, 'name': 'queue10', 'consumers': 0, 'vhost': '/', 'backing_queue_status': {'q1': 0, 'q3': 0, 'q2': 0, 'q4': 0, 'avg_ack_egress_rate': 0.0, 'ram_msg_count': 0, 'ram_ack_count': 0, 'len': 0, 'persistent_count': 0, 'target_ram_count': 'infinity', 'next_seq_id': 0, 'delta': ['delta', 'undefined', 0, 'undefined'], 'pending_acks': 0, 'avg_ack_ingress_rate': 0.0, 'avg_egress_rate': 0.0, 'avg_ingress_rate': 0.0}, 'durable': True, 'idle_since': '2013-10-03 13:38:18', 'exclusive_consumer_tag': '', 'arguments': {}, 'memory': 10956, 'policy': '', 'auto_delete': False},
            {'status': 'running', 'node': 'rabbit@host', 'name': 'queue11', 'consumers': 0, 'vhost': '/', 'backing_queue_status': {'q1': 0, 'q3': 0, 'q2': 0, 'q4': 0, 'avg_ack_egress_rate': 0.0, 'ram_msg_count': 0, 'ram_ack_count': 0, 'len': 0, 'persistent_count': 0, 'target_ram_count': 'infinity', 'next_seq_id': 0, 'delta': ['delta', 'undefined', 0, 'undefined'], 'pending_acks': 0, 'avg_ack_ingress_rate': 0.0, 'avg_egress_rate': 0.0, 'avg_ingress_rate': 0.0}, 'durable': True, 'idle_since': '2013-10-03 13:38:18', 'exclusive_consumer_tag': '', 'arguments': {}, 'memory': 10956, 'policy': '', 'auto_delete': False},
            ...
        ]
        """
        if len(explicit_filters) > max_detailed:
            raise Exception(
                "The maximum number of %s you can specify is %d." % (object_type, max_detailed))

        # a list of queues/nodes is specified. We process only those
        if explicit_filters or regex_filters:
            matching_lines = []
            for data_line in data:
                name = data_line.get("name")
                if name in explicit_filters:
                    matching_lines.append(data_line)
                    explicit_filters.remove(name)
                    continue

                match_found = False
                for p in regex_filters:
                    match = re.search(p, name)
                    if match:
                        if match.groups():
                            data_line["queue_family"] = match.groups()[0]
                        matching_lines.append(data_line)
                        match_found = True
                        break

                if match_found:
                    continue

                # Absolute names work only for queues
                if object_type != QUEUE_TYPE:
                    continue
                absolute_name = '%s/%s' % (data_line.get("vhost"), name)
                if absolute_name in explicit_filters:
                    matching_lines.append(data_line)
                    explicit_filters.remove(absolute_name)
                    continue

                for p in regex_filters:
                    match = re.search(p, absolute_name)
                    if match:
                        if match.groups():
                            data_line["queue_family"] = match.groups()[0]
                        matching_lines.append(data_line)
                        match_found = True
                        break

                if match_found:
                    continue

            data = matching_lines

        for data_line in data[:max_detailed]:
            # We truncate the list of nodes/queues if it's above the limit
            self._get_metrics(data_line, object_type, custom_tags)

    def _get_metrics(self, data, object_type, custom_tags):
        tags = []
        tag_list = TAGS_MAP[object_type]
        for t in tag_list:
            try:
                tag = remove_invalid_characters(data.get(t))
                if tag:
                    tag = ('%s_%s=%s' % (TAG_PREFIX, tag_list[t], tag)).encode('utf-8')
                    tags.append(tag)
            except Exception as e:
                msg = "Warning executing _get_metrics: {}. it is because of missing {}. " \
                      "You can specify the regular regression in conf.".format(e, t)
                self.log_warn(msg)

        tags = str(tags)[1:-1].replace("'", "").replace(",", "")
        for attribute, metric_name, operation in ATTRIBUTES[object_type]:
            # Walk down through the data path, e.g. foo/bar => d['foo']['bar']
            root = data
            keys = attribute.split('/')
            for path in keys[:-1]:
                root = root.get(path, {})

            value = root.get(keys[-1], None)

            if type(value) == unicode:
                value = None

            ts = time.time()
            if value is not None:
                try:
                    self._readq.nput('rabbitmq.%s.%s %d %d %s' %
                                     (METRIC_SUFFIX[object_type], metric_name, ts, operation(value), tags))
                except ValueError as e:
                    self.log_error("Caught ValueError for %s %s = %s  with tags: %s" % (
                        METRIC_SUFFIX[object_type], attribute, value, str(tags)))
                    self.log_error(e)

    def get_connections_stat(self, instance, base_url, object_type, vhosts, custom_tags, auth=None, ssl_verify=True):
        ts = time.time()
        """
        Collect metrics on currently open connection per vhost.
        """

        data = self._get_data(urlparse.urljoin(base_url, object_type), auth=auth,
                              ssl_verify=ssl_verify)

        stats = {vhost: 0 for vhost in vhosts}
        connection_states = defaultdict(int)
        for conn in data:
            if conn['vhost'] in vhosts:
                stats[conn['vhost']] += 1
                connection_states[conn['state']] += 1

        for vhost, nb_conn in stats.iteritems():
            self._readq.nput('rabbitmq.connections %d %d %s' % (ts, nb_conn, custom_tags))

        for conn_state, nb_conn in connection_states.iteritems():
            self._readq.nput('rabbitmq.connections.state %d %d %s' % (ts, nb_conn, custom_tags))

    def _check_aliveness(self, instance, base_url, vhosts, custom_tags, auth=None, ssl_verify=True):
        """
        Check the aliveness API against all or a subset of vhosts. The API
        will return {"status": "ok"} and a 200 response code in the case
        that the check passes.
        """

        for vhost in vhosts:
            ts = time.time()
            tags = str(['vhost:%s' % vhost]) + custom_tags
            # We need to urlencode the vhost because it can be '/'.
            path = u'aliveness-test/%s' % (urllib.quote_plus(vhost))
            aliveness_url = urlparse.urljoin(base_url, path)
            # aliveness_proxy = self.get_instance_proxy(instance, aliveness_url)
            aliveness_response = self._get_data(aliveness_url, auth=auth, ssl_verify=ssl_verify)
            message = u"Response from aliveness API: %s" % aliveness_response

            if aliveness_response.get('status') == 'ok':
                status = 0
                self._readq.nput('rabbitmq.aliveness %d %d %s' % (ts, status, custom_tags))
            else:
                status = 1
                self._readq.nput('rabbitmq.aliveness %d %d %s' % (ts, status, custom_tags))
