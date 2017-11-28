""" convert a (bz2 compressed) file of pgn chess games to a sqlite3 database,
with appropriate column indexing
"""

import os
import bz2
import sys
import glob
import itertools

import pandas as pd

from lichess.util import printprogress, DATAROOT, GAME_FIELDS
from lichess.util import pgn2dict, read_til_break
from lichess import sql_io


def build_insert(game):
    """ build the sql statement for an insert
    """
    row = ""
    for field in GAME_FIELDS:
        if field[0] not in game.keys():
            game[field[0]] = {True: "-1", False: "NA"}[field[1] == "real"]

        if field[1] == "real":
            row += game[field[0]]
        else:
            row += "'" + game[field[0]] + "'"
        row += ", "

    return row[:-2]


def process_one_row(pgn, cdb):
    """ process_one_row """

    parsed = read_til_break(pgn)
    moves = read_til_break(pgn)

    if not parsed or not moves:
        return

    game = pgn2dict(parsed)
    game['Moves'] = moves[0]

    row = build_insert(game)
    if "?" in row:
        return True
    cdb.execute('INSERT INTO games VALUES (%s)' % (row))
    return True


def process_one_file(pgn, cdb):
    """ iterate over games until done
    """

    # setup sql db
    print("connecting to db %s..." % (cdb.name))

    # loop over games
    countr = itertools.count()
    while process_one_row(pgn, cdb) is not None:
        cur = next(countr)
        if cur % 1000 == 0:
            printprogress(
                "%s: %d processed..."
                % (__file__, cur))

    print('complete')


def create_games_table(cdb, pgn_glob="*bz2"):
    """ create_games_table """

    pgn_files = glob.glob(os.path.join(DATAROOT, "bz2", pgn_glob))
    cdb.execute(
        'CREATE TABLE IF NOT EXISTS games (%s)'
        % (', '.join([' '.join(column) for column in GAME_FIELDS])))

    for pgn_file in pgn_files:
        print("begin %s..." % (pgn_file))
        bzf = bz2.BZ2File(pgn_file)
        process_one_file(bzf, cdb)
        bzf.close()


def create_players_table(cdb):
    """ create_games_indices """
    cdb.execute("DROP TABLE IF EXISTS players ")

    cdb.execute(
        "CREATE TABLE IF NOT EXISTS players " +
        "(Name text, N_Game real)")

    cdb.execute("SELECT DISTINCT White FROM games")
    players = cdb.curs.fetchall()

    countr = itertools.count()
    n_player = len(players)
    for player in players:
        cdb.execute(
            "SELECT COUNT(*) from games WHERE White = '%s'" % (player[0]),
            timed=False)
        count = cdb.fetchone()

        cdb.execute(
            "SELECT COUNT(*) from games WHERE Black = '%s'" % (player[0]),
            timed=False)
        count += cdb.fetchone()

        cdb.execute(
            "INSERT INTO players VALUES ('%s', '%d')"
            % (player[0], count), timed=False)

        cur = next(countr)
        if cur % 100 == 0:
            printprogress(
                "%s: %.2f percent processed..."
                % ('create_players_table', float(cur) / n_player * 100))

    cdb.execute("CREATE INDEX IF NOT EXISTS gidx on players(N_Game)")


def create_superplayers(cdb):
    """ create_superplayers creates a games table containing the top N players
    by game count"""
    cdb.execute('DROP TABLE IF EXISTS superplayer_games')
    cdb.query2dataframe("SELECT * from players ORDER BY N_Game DESC")
    n_top = 20
    top = cdb.dataframe.head(n_top)
    data = pd.DataFrame()

    for idx in range(n_top):
        i_super = top.loc[idx]
        for side in ['White', 'Black']:
            cdb.query2dataframe(
                "SELECT * from games WHERE %s = '%s'"
                % (side, i_super.Name))
            data = data.append(cdb.dataframe)

    data.to_sql("superplayer_games", cdb.conn)


def create_games_indices(cdb, table='games'):
    """ create_games_indices add relevant indexing"""

    cdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_white_elo on %s(WhiteElo)" % (table))
    cdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_black_elo on %s(BlackElo)" % (table))
    cdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_white on %s(White)" % (table))
    cdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_black on %s(Black)" % (table))


def main():
    """ main """
    cdb = sql_io.ChessDB()
    functor = {
        "games": create_games_table,
        "indices": create_games_indices,
        "players": create_players_table,
        "superplayers": create_superplayers
    }
    functor[sys.argv[1]](cdb)
    cdb.cleanup()


if __name__ == "__main__":
    main()
