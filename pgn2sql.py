#!/usr/bin/env python

import multiprocessing
import re
import sys
import time
import sqlite3

DEBUG = True
NPROC = 4
FIELDS = [
    ('UTCDate', 'text'),
    ('Moves', 'text'),
    ('Opening', 'text'),
    ('ECO', 'text'),
    ('WhiteElo', 'real'),
    ('Site', 'text'),
    ('BlackElo', 'real'),
    ('UTCTime', 'text'),
    ('Black', 'text'),
    ('Result', 'text'),
    ('WhiteRatingDiff', 'real'),
    ('Termination', 'text'),
    ('BlackRatingDiff', 'real'),
    ('White', 'text'),
    ('TimeControl', 'text'),
    ('Event', 'text')]


def pgn2dict(txt):
    """ using regex, convert pgn game header into a dict
    """
    result = {}
    for line in txt:
        if not line:
            continue
        match = re.search(r'(\w+) "(.*)"', line).groups()
        result[match[0]] = match[1].replace("'", "''")

    return result


def consume_newlines(fptr, nnl):
    n = 0
    line = fptr.readline()
    while line and n < nnl:
        line = fptr.readline()
        if line == "\n":
            n += 1


def read_til_break(fptr):
    """ read all lines into list until encounter newline
    """
    line = fptr.readline()
    txt = [line.strip()]
    while line and line != '\n':
        line = fptr.readline()
        txt.append(line.strip())

    return [entry for entry in txt if entry]


def printprogress(msg):
    """ printprogress
    """
    # print("\033c")  # clear console
    sys.stdout.write("\r" + msg)
    sys.stdout.flush()


def build_insert(game):
    """ build the sql statement for an insert
    """
    row = ""
    for field in FIELDS:
        if field[0] not in game.keys():
            game[field[0]] = {True: "-1", False: "NA"}[field[1] == "real"]

        if field[1] == "real":
            row += game[field[0]]
        else:
            row += "'" + game[field[0]] + "'"
        row += ", "

    return row[:-2]


def worker(pgn, worker_index, n_game):
    """ multiprocessing target
    orient to correct location in file, create a unique sql connection
    and iterate over games until done
    """

    # orient to location in file
    pgn.seek(0)
    n_skip_games = n_game * worker_index
    print("finding game %d..." % (n_skip_games))
    consume_newlines(pgn, n_skip_games * 2)

    # setup sql db
    dbname = sys.argv[1].replace('.pgn', '-worker%0.2d.sql' % (worker_index))
    print("connecting to db %s..." % (dbname))
    sql = sqlite3.connect(dbname)
    cursor = sql.cursor()
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS games (%s)'
        % (', '.join([' '.join(column) for column in FIELDS])))

    # loop over games
    for i in range(n_game):
        if i % 100 == 0:
            printprogress(
                "%s Worker %d: %.2f percent..."
                % (__file__, worker_index, 100 * float(i) / n_game))
        game = pgn2dict(read_til_break(pgn))
        game['Moves'] = read_til_break(pgn)[0]

        try:
            row = build_insert(game)
            if "?" in row:
                continue
            cmd = 'INSERT INTO games VALUES (%s)' % (row)
            cursor.execute(cmd)
        except Exception as e:
            import bpdb; bpdb.set_trace()
            raise(e)

    print('complete')
    # clean up
    sql.commit()
    sql.close()


def count_games(fptr):
    count = 0
    line = fptr.readline()
    while line:
        if line == "\n":
            count += 1
        line = fptr.readline()
        if count % 20000 == 0:
            printprogress("found %9d games in %s..." % (count / 2, fptr.name))

    printprogress("found %9d games in %s..." % (count / 2, fptr.name))
    print('complete')
    # two newlines per game
    n_game = count / 2
    n_game = n_game - (n_game % NPROC)
    return n_game


def main(pgn):
    """ pgn2sql main
    """

    n_game = count_games(pgn)

    # create multiple processes
    procs = [
        multiprocessing.Process(
            target=worker,
            args=(pgn, idx, n_game / NPROC))
        for idx in range(NPROC)]

    # start the processes and monitor
    for proc in procs:
        proc.start()

    while any([proc.is_alive() for proc in procs]):
        time.sleep(1)


if __name__ == "__main__":
    pgn = open(sys.argv[1])

    if DEBUG:
        worker(pgn, 0, count_games(pgn))
    else:
        main(pgn)

    pgn.close()
