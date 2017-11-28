""" a module to simplify interction with sql database
"""

import os
import sys
import time
import sqlite3

import pandas as pd
import numpy as np

from lichess.util import DATAROOT, DBNAME, QUIET


class ChessDB(object):
    """ chess database """

    def __init__(self, dbname=DBNAME):
        self.name = dbname
        self.root = DATAROOT
        self.dataframe = pd.DataFrame()
        self.conn = sqlite3.connect(os.path.join(self.root, self.name))
        self.curs = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def cleanup(self):
        """ cleanup """
        self.conn.commit()
        self.conn.close()

    def query2dataframe(self, query, timed=True):
        """ query2dataframe """
        if timed:
            self.dataframe = exec_print(pd.read_sql, query, self.conn)
        else:
            self.dataframe = pd.read_sql(query, self.conn)

    def execute(self, query, timed=True):
        """ sql_execute """
        if timed:
            exec_print(self.curs.execute, query)
        else:
            self.curs.execute(query)

    def fetchone(self):
        """ fetch """
        return self.curs.fetchone()[0]

    def fetcharray(self):
        """ fetcharray """
        return np.array(self.curs.fetchall())


def exec_print(exec_func, *args):
    """ exec_print """
    if not QUIET:
        sys.stdout.write(
            "executing \n\t%s\n\tat %s..." % (args[0], time.ctime()))
        sys.stdout.flush()
        t_start = time.time()
        result = exec_func(*args)
        sys.stdout.write(
            " complete: %.2f seconds elapsed...\n"
            % (time.time() - t_start))
        sys.stdout.flush()
    else:
        return
    return result
