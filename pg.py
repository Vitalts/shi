# -*- coding: utf-8 -*-


__version__ = '0.2'


import sys
import cStringIO
import psycopg2


def pg_quoted_str_as_is(s):
    return psycopg2.extensions.adapt(s).getquoted()


def pg_quoted_str(s):
    return psycopg2.extensions.adapt(s.encode("utf-8")).getquoted().decode("utf-8")


# noinspection PyBroadException
class pg(object):
    __connection_string = ""
    __connection = None
    __cursor = None

    def __init__(self, *args):
        if len(args) == 1:
            self.__connection_string = args[0]
        elif len(args) == 2:
            f = open(args[0], 'r')
            for line in f.readlines():
                line = line.replace("\r", "").strip(" \t")
                if line[0] != "#":
                    line = line.split(":")
                    if (len(line) >= 2) and (args[1] == line[0].strip(" \t")):
                        self.__connection_string = line[1].strip(" \t")
            f.close()
        elif len(args) == 4:
            # noinspection SpellCheckingInspection
            preset = "host='%s' dbname='%s' user='%s' password='%s'"
            self.__connection_string = preset % args
        else:
            self.__print_error("Unknown initialization type")

    def __del__(self):
        self.disconnect()

    @staticmethod
    def __print_error(message):
        _e_type, e_value, _e_traceback = sys.exc_info()
        print message + "\n-> %s" % e_value

    def connected(self):
        return self.__connection is not None

    def connect(self):
        if not self.connected():
            try:
                self.__connection = psycopg2.connect(self.__connection_string)
                self.__connection.autocommit = True
                self.__cursor = self.__connection.cursor()
                return self.__connection is not None
            except Exception, _:
                self.__print_error("Database connection failed!")
                raise

    def disconnect(self):
        if self.connected():
            self.__connection.close()
        self.__connection = None
        self.__cursor = None

    def sql_exec(self, sql, header=False):
        self.connect()
        self.__cursor.execute(sql)
        try:
            data = [r for r in self.__cursor]
        except Exception, _:
            data = []

        if header:
            data.insert(0, [desc[0] for desc in self.__cursor.description])
        return data

    def copy_from(self, source, table, sep="\t", null="\\N", size=8192, columns=None):
        """
        copy source into database table
        """
        self.connect()

        if isinstance(source, list):
            string_stream = cStringIO.StringIO("\n".join([sep.join(["%s" % (null if r is None else r)
                                                                   for r in row]) for row in source]))
            return self.__cursor.copy_from(string_stream, table, sep, null, size, columns)
        elif isinstance(source, (str, unicode)):
            return self.__cursor.copy_from(cStringIO.StringIO(source), table, sep, null, size, columns)
        elif isinstance(source, cStringIO.InputType):
            return self.__cursor.copy_from(source, table, sep, null, size, columns)
        else:
            return self.__cursor.copy_from(source, table, sep, null, size, columns)

    # noinspection PyShadowingBuiltins
    def copy_to(self, file, table, sep="\t", null="\\N", columns=None):
        self.connect()
        return self.__cursor.copy_to(file, table, sep, null, columns)

    # noinspection PyShadowingBuiltins
    def copy_expert(self, sql, file, size=8192):
        self.connect()
        return self.__cursor.copy_expert(sql, file, size)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()
