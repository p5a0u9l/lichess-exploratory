""" pandas interface to pgn sql db
"""

import os
import sys

import matplotlib.pyplot as plt
import numpy as np

from lichess import sql_io
from lichess.util import DATAROOT, MINELO, MAXELO

ROWDIM, COLDIM = 0, 1
plt.switch_backend("agg")


def elo_bins(cdb):
    """ elo_bins """
    range_step = 50
    ranges = range(MINELO, MAXELO, range_step)
    bins = {}

    #  compute
    for elo in ranges:
        elo_range = (elo, elo + range_step)
        cdb.execute(
            "SELECT COUNT(*) from games WHERE WhiteElo > " +
            "%d and WhiteElo <= %d" % elo_range)
        bins[elo] = cdb.fetchone()

    return bins


def plot_hist_name_players(cdb, savename):
    """ plot_hist """
    cdb.execute("SELECT N_Game from players ORDER BY N_Game DESC")
    values = cdb.fetcharray()

    total = len(values)
    xval = np.arange(total)

    plt.figure(figsize=(24, 12))
    plt.plot(xval, values, '.-')
    plt.ylabel('probability density')
    plt.xlabel('distint player index')
    plt.gca().grid(alpha=0.2)

    plt.suptitle('distribution of Game Count over {:,} players'.format(total))
    plt.savefig(os.path.join(DATAROOT, "plots", savename))


def plot_hist_elo_games(cdb, savename):
    """ plot_hist """
    bins = elo_bins(cdb)

    yval = np.array(list(bins.values()), dtype=np.float)
    total = sum(yval)
    yval /= np.float(total)
    xval = np.arange(yval.shape[0])

    plt.figure(figsize=(24, 8))
    plt.subplot(121)
    plt.bar(xval, yval)
    plt.xticks(xval + 0.5, bins.keys(), rotation=90)
    plt.ylabel('probability density')
    plt.xlabel('white ELO')
    plt.gca().grid(alpha=0.2)

    plt.subplot(122)
    plt.bar(xval, np.cumsum(yval))
    plt.xticks(xval + 0.5, bins.keys(), rotation=90)
    plt.ylabel('cumulative density')
    plt.xlabel('white ELO')
    plt.gca().grid(alpha=0.2)

    plt.suptitle('distribution of White ELO over {:,} games'.format(total))
    plt.savefig(os.path.join(DATAROOT, "plots", savename))


def main():
    """ main """
    cdb = sql_io.ChessDB()

    if sys.argv[1] == "elo-games":
        plot_hist_elo_games(cdb, "white_elo_distribution.png")

    elif sys.argv[1] == "name-players":
        plot_hist_name_players(cdb, "white_count_distribution.png")

    else:
        print("unknown slice obtion %s..." % (sys.argv[1]))


if __name__ == "__main__":
    main()
