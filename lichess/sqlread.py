"""
pandas interface to pgn sql db
"""

import os
import glob
import sqlite3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from lichess.util import printprogress, DATAROOT

ROWDIM, COLDIM = 0, 1


def elo_bins(conn):
    """ elo_bins """
    range_step = 100
    ranges = range(500, 3300, range_step)
    height = {}

    #  compute
    for elo in ranges:
        elo_range = (elo, elo + range_step)
        printprogress("binning elo: %d to %d..." % elo_range)
        query = "SELECT WhiteElo from games WHERE WhiteElo > " + \
            "%d and WhiteElo <= %d" % elo_range
        dframe = pd.read_sql(query, conn)
        height['%d' % elo] = dframe.shape[ROWDIM]

    # plot
    yval = np.array(list(height.values()))
    xval = np.arange(yval.shape[0])
    plt.bar(xval, yval)
    plt.xticks(xval + 0.5, height.keys(), rotation=90)
    plt.ylabel('# games')
    plt.title('distribution of White ELO')
    plt.gca().grid(alpha=0.2)
    plt.savefig("elo_distribution.png", dpi=300)

    return height


def main():
    """ main """
    plt.switch_backend("agg")
    dbs = glob.glob(os.path.join(DATAROOT, "*.sql"))
    conn = sqlite3.connect(dbs[0])
    heights = elo_bins(conn)


if __name__ == "__main__":
    main()
