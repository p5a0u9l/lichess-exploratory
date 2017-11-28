""" pandas interface to pgn sql db
"""

import os
import time
import sys

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from lichess import sql_io
from lichess.util import DATAROOT, MINELO, MAXELO, get_utc_dict

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


def cull_superplayers(cdb):
    """ cull_superplayers """
    cdb.query2dataframe("SELECT * from players ORDER BY N_Game DESC")
    players = cdb.dataframe
    players = players[players.N_Game < 10000 and players.N_Game > 10]
    import bpdb; bpdb.set_trace()


def superplayers(cdb):
    """ superplayers """
    cdb.query2dataframe("SELECT * from players ORDER BY N_Game DESC")
    players = cdb.dataframe

    figs = [plt.figure(figsize=(26, 14)), plt.figure(figsize=(26, 14))]
    frame = pd.DataFrame()

    for idx in range(12):
        i_super = players.loc[idx]
        for side in ['White', 'Black']:
            cdb.query2dataframe(
                "SELECT * from superplayer_games WHERE %s = '%s'"
                % (side, i_super.Name), timed=False)
            frame = frame.append(cdb.dataframe)

        datecounts = frame.UTCDate.value_counts(sort=False)
        dates = get_utc_dict(datecounts.keys())
        dcs = datecounts.values

        plt.figure(figs[0].number)
        plt.subplot(3, 4, idx + 1)
        plt.plot_date(dates, dcs, ms=1)
        plt.xticks(rotation=45)
        plt.ylabel('games played per day')
        plt.gca().grid(alpha=0.2)
        day_avg = sum(dcs) / float(i_super.N_Game)
        plt.gca().axhline(
            day_avg, np.min(dates), np.max(dates))
        plt.title('%s %d total' % (i_super.Name, i_super.N_Game))

        plt.figure(figs[1].number)
        plt.subplot(3, 4, idx + 1)
        plt.hist(dcs)
        plt.ylabel('%s - number of games' % (i_super.Name))
        plt.xlabel('games played per day')
        plt.gca().grid(alpha=0.2)

    plt.figure(figs[0].number)
    plt.suptitle(
        'super player analysis %s' % (time.ctime()))
    plt.savefig(
        os.path.join(DATAROOT, "plots", "superplayers_bydate.png"))

    plt.figure(figs[1].number)
    plt.savefig(
        os.path.join(DATAROOT, "plots", "superplayers_pdfcdf.png"))


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

    elif sys.argv[1] == "super-players":
        superplayers(cdb)

    else:
        print("unknown slice obtion %s..." % (sys.argv[1]))


if __name__ == "__main__":
    main()
