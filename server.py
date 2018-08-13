import BaseHTTPServer
import cgi
import urlparse
import os
import json
import logging
import sys
import pg
import datetime


class ServerHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    main http server handler class
    """
    def __init__(self, db_conn, sql_select, sql_insert, *args):
        self._log = logging.getLogger('Handler')

        self._db_conn = db_conn
        self._sql_select = sql_select
        self._sql_insert = sql_insert

        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args)

    def do_HEAD(self):
        """
        HTTP HEAD
        """
        self._log.info('do_HEAD')
        print 'head', self.path
        self.send_head()

    def do_GET(self):
        """
        HTTP GET
        """
        default_params = {'from': [datetime.datetime.today().strftime('%Y-%m-%d')],
                          'to': [(datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')]}
        params = urlparse.parse_qs(urlparse.urlparse(self.path).query)
        for k, v in default_params.iteritems():
            if k not in params:
                params[k] = v[:]

        for r in ['device', 'measure', 'from', 'to']:
            if r not in params:
                err_msg = 'param %s not specified' % r
                self.send_head(code=400, message=err_msg)
                self.wfile.write(err_msg)
                return

        headers = ['device', 'timestamp']
        headers.extend(params['measure'][:])

        data = [['%s' % r for r in row] for row in self._select_measurements(devices=params['device'],
                                                                             measurements=params['measure'],
                                                                             date_from=params['from'][0],
                                                                             date_to=params['to'][0])]

        data.insert(0, headers[:])

        self.send_head()
        self.wfile.write('\r\n'.join(['\t'.join(row) for row in data]))

    def do_POST(self):
        """
        HTTP POST
        """
        c_type, p_dict = cgi.parse_header(self.headers.getheader("content-type"))
        if c_type == "multipart/form-data":
            data = cgi.parse_multipart(self.rfile, p_dict)
        else:
            length = int(self.headers.getheader("content-length"))
            data = urlparse.parse_qs(self.rfile.read(length), keep_blank_values=1)

        data = json.loads(json.dumps(data))

        measurements = []
        for key in data.iterkeys():
            if key.startswith('"') and key.endswith('"'):
                key = key[1:-1]
            sep = '\\n' if '\\n' in key else '\n'
            measurement = self.parse_measurement(key.split(sep))
            if not isinstance(measurement, dict):
                self.send_head(code=400, message=measurement)
                self.wfile.write(measurement)
                return
            measurements.append(measurement)

        self._insert_measurements(measurements)

        self.send_head()
        self.wfile.write('accepted')

    def send_head(self, code=200, message=None, cookie=''):
        """
        send header
        """
        self.send_response(code, message)
        if code == 200 and cookie:
            self.send_header('Set-Cookie', cookie)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def _select_measurements(self, devices, measurements, date_from=None, date_to=None):
        pg_conn = pg.pg(" ".join(["%s='%s'" % (k, v) for k, v in self._db_conn.iteritems()]))
        return pg_conn.sql_exec(self._compose_select_sql(devices, measurements, date_from, date_to))

    def _insert_measurements(self, measurements):
        if not isinstance(measurements, (list, tuple)):
            measurements = [measurements]

        pg_conn = pg.pg(" ".join(["%s='%s'" % (k, v) for k, v in self._db_conn.iteritems()]))
        for measurement in measurements:
            data = []
            for k, v in measurement['measures'].iteritems():
                data.append([measurement['device'], k, v])
            pg_conn.sql_exec(self._compose_insert_sql(data))

    def _compose_insert_sql(self, data):
        lines = ['select %s' % (', '.join([("'%s'" % rr) if i < 2 else rr for rr in r])) for i, r in enumerate(data)]
        return self._sql_insert.replace('%DATA%', '\nunion all\n'.join(lines))

    @staticmethod
    def _prepare_select_sql_parts(devices, measurements, date_from=None, date_to=None):
        if not isinstance(devices, (list, tuple)):
            devices = [devices]
        if not isinstance(measurements, (list, tuple)):
            measurements = [measurements]

        measurements_select = ', '.join(['%M%.value as %M%'.replace('%M%', r) for r in measurements])
        if measurements_select:
            measurements_select = ', %s' % measurements_select

        measurement_part = 'left join measurements.data %M% on %M%.id = m.id and %M%.measure = \'%M%\''
        measurements_join = '\r\n'.join([measurement_part.replace('%M%', r) for r in measurements])
        devices_where = 'and device.name in (%s)' % ', '.join(["'%s'" % r for r in devices])
        # noinspection SpellCheckingInspection
        date_from_where = ('and tstamp >= \'%s\'::timestamp with time zone' % date_from) if date_from else ''
        # noinspection SpellCheckingInspection
        date_to_where = ('and tstamp < \'%s\'::timestamp with time zone' % date_to) if date_to else ''

        return [('%MEASUREMENTS_SELECT%', measurements_select),
                ('%MEASUREMENTS_JOIN%', measurements_join),
                ('%DEVICES%', devices_where),
                ('%DATE_FROM%', date_from_where),
                ('%DATE_TO%', date_to_where)]

    def _compose_select_sql(self, devices, measurements, date_from=None, date_to=None):
        sql = self._sql_select
        for r in self._prepare_select_sql_parts(devices, measurements, date_from, date_to):
            sql = sql.replace(r[0], r[1])

        return sql

    @staticmethod
    def parse_measurement(data):
        """
        :param data: [line1, ...]
        :return: {device: name, token: device_token, measures: {measure: value, ...}} or error text
        """
        result = {}
        for i, r in enumerate(data):
            if not r:
                continue
            measure = r.split(':')
            if len(measure) != 2:
                return 'invalid data format'
            rep = '\\r' if '\\r' in measure[1] else '\r'
            if i == 0:
                result = {'device': measure[0].strip(), 'token': measure[1].replace(rep, '').strip(), 'measures': {}}
            else:
                result['measures'][measure[0].strip()] = measure[1].replace(rep, '').strip()
        return result


# noinspection SpellCheckingInspection
class Server(object):
    """
    main http server class
    """
    def __new__(cls, host, port, db_conn):
        return super(Server, cls).__new__(cls)

    def __init__(self, host, port, db_conn):
        def handler(*args):
            ServerHandler(self._db_conn, self._sql_select, self._sql_insert, *args)

        self._log = logging.getLogger('Server')
        self._log.info('init (%s, %s)' % (host, port))

        self._host = host
        self._port = int(port)
        self._db_conn = db_conn

        self._path = os.path.dirname(os.path.abspath(__file__))
        self._sql_init, self._sql_select, self._sql_insert = self._get_sqls()

        self._server = BaseHTTPServer.HTTPServer
        self._httpd = self._server((self._host, self._port), handler)

    def _get_sqls(self):
        def read_file(file_name):
            with open(file_name, 'r') as f:
                return f.read()

        return [read_file(os.path.join(self._path, '%s.sql' % r)) for r in ['init', 'select', 'insert']]

    def run(self):
        self._log.info('run (%s, %s)' % (self._host, self._port))
        self._httpd.serve_forever()


def main():
    def complete_dict(dest, source):
        result = {k: v for k, v in dest.iteritems()}
        for k, v in source.iteritems():
            if k not in result:
                result[k] = v
            elif isinstance(v, dict):
                result[k] = complete_dict(result[k] if isinstance(result[k], dict) else {}, v)
        return result

    main_file_name = '.'.join(os.path.abspath(__file__).split('.')[:-1])
    config_file = '%s.%s' % (main_file_name, 'json')
    # noinspection SpellCheckingInspection
    default_config = {"log level": os.environ.get('log_level', 'debug'),
                      'server': {'host': os.environ.get('host', '0.0.0.0'),
                                 'port': os.environ.get('port', '8080')},
                      'db': {'host': os.environ.get('db_host', '127.0.0.1'),
                             'port': int(os.environ.get('db_port', '5432')),
                             'dbname': os.environ.get('db_name', 'measurement'),
                             'user': os.environ.get('db_user', 'user'),
                             'password': os.environ.get('db_pass', 'password')}
                      }

    if os.path.isfile(config_file):
        with open(config_file, 'r') as f:
            config = json.loads(f.read())
    else:
        config = default_config
    config = complete_dict(config, default_config)

    # noinspection SpellCheckingInspection
    frm = '%(asctime)s %(name)s %(levelname)s %(message)s'
    # noinspection SpellCheckingInspection
    datefmt = '%y.%m.%d %H:%M:%S'

    logging.basicConfig(filename='%s.%s' % (main_file_name, 'log'),
                        level=config['log level'].upper(),
                        format=frm,
                        datefmt=datefmt)

    lh_stderr = logging.StreamHandler(sys.stderr)
    lh_stderr.setLevel(config["log level"].upper())
    lh_stderr.setFormatter(logging.Formatter(fmt=frm, datefmt=datefmt))

    log_root = logging.getLogger()
    log_root.setLevel(config["log level"].upper())
    log_root.addHandler(lh_stderr)

    server = Server(config['server']['host'], config['server']['port'], config['db'])
    server.run()


if __name__ == '__main__':
    main()
