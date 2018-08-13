import logging


log = logging.getLogger(__name__)


def connect(conn_str):
    return Connection(conn_str)


class Cursor(object):
    def __new__(cls):
        return super(Cursor, cls).__new__(cls)

    def __init__(self):
        log.debug('Cursor init')

    @staticmethod
    def execute(sql):
        log.debug('execute %s' % sql)


class Connection(object):
    def __new__(cls, conn_str):
        return super(Connection, cls).__new__(cls)

    def __init__(self, conn_str):
        log.debug('connection init(\'%s\')' % conn_str)
        self.conn_str = conn_str

    @staticmethod
    def cursor():
        return Cursor()

    def close(self):
        pass
