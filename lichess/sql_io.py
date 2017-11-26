""" a module to simplify interction with sql database
"""

import os
import sys
import time
import sqlite3

import numpy as np

from lichess.util import DATAROOT, DBNAME


class ChessDB(object):
    """ chess database """
    def __init__(self):
        self.conn = sqlite3.connect(os.path.join(DATAROOT, DBNAME))
        self.curs = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def cleanup(self):
        self.conn.commit()
        self.conn.close()

    def execute(self, query, timed=True):
        """ sql_execute """
        if timed:
            sys.stdout.write(
                "executing \n\t%s\n\tat %s..." % (query, time.ctime()))
            sys.stdout.flush()
            t_start = time.time()

        self.curs.execute(query)

        if timed:
            sys.stdout.write(
                " complete: %.2f seconds elapsed...\n"
                % (time.time() - t_start))
            sys.stdout.flush()

    def fetchone(self):
        """ fetch """
        return self.curs.fetchone()[0]

    def fetcharray(self):
        """ fetcharray """
        return np.array(self.curs.fetchall())


