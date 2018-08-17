import unittest
import subprocess
import os
import signal
import requests
import shutil


# noinspection PyAttributeOutsideInit,PyPep8Naming
class TestServer(unittest.TestCase):
    def setUp(self):
        # paths
        self._path = os.path.dirname(os.path.abspath(__file__))
        self._server_path = os.path.join(*os.path.split(self._path)[:-1])

        # set test os.environs
        self._os_environ_test = {'log_level': 'debug',
                                 'port': '8080'}
        self._os_environ = {k: os.environ.get(k, None) for k in self._os_environ_test.iterkeys()}
        for k, v in self._os_environ_test.iteritems():
            os.environ[k] = v

        # mock psycopg2
        self._module_psycopg2_dest = '../psycopg2.py'
        shutil.copy(self._module_psycopg2_dest[1:], self._module_psycopg2_dest)
        self._server = subprocess.Popen(['python', os.path.join(self._server_path, 'server.py')])

    def tearDown(self):
        # kill server
        os.kill(self._server.pid, signal.SIGTERM)

        self._path = ''

        # rollback os.environs
        for k, v in self._os_environ.iteritems():
            if v is None:
                del os.environ[k]
            else:
                os.environ[k] = v

        # remove psycopg2
        for r in [self._module_psycopg2_dest, '%sc' % self._module_psycopg2_dest]:
            if os.path.isfile(r):
                os.remove(r)

    def test_all(self):
        self._test_send()
        self._test_select()

    # noinspection PyPep8,PyUnnecessaryBackslash
    def _test_send(self):
        request_data = ['DEVICE1: 4087783e-5241-44ef-b915-6df54c6a045a' \
                        'C: 14.7\n' \
                        'V: 2.8\n',
                        'DEVICE1: 4087783e-5241-44ef-b915-6df54c6a045a\n' \
                        'C: 14.7\n' \
                        'V: 2.8\n',
                        'DEVICE1: 4087783e-5241-44ef-b915-6df54c6a045a\n' \
                        'C: 14.7\n' \
                        'V 2.8\n']
        response_data = [(400, 'invalid data format'), (200, 'accepted'), (400, 'invalid data format')]

        for i, request in enumerate(request_data):
            r = requests.post('http://127.0.0.1:8080', json=request)
            self.assertEqual(response_data[i], (r.status_code, r.content))

    def _test_select(self):
        url = 'http://127.0.0.1:8080/?device=test1&device=test2&measure=C&measure=V&from=2018-05-01'
        r = requests.get(url)
        self.assertEqual('device\ttimestamp\tC\tV', r.content)

        r = requests.get('http://127.0.0.1:8080', params={'device': ['test1', 'test2'],
                                                          'measure': ['C', 'V'],
                                                          'from': '2018-05-01'})
        self.assertEqual('device\ttimestamp\tC\tV', r.content)


if __name__ == '__main__':
    unittest.main()
