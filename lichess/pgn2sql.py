""" convert a file of pgn chess games to a sqlite3 database
"""

import re
import os
import glob
import bz2
import sqlite3
import itertools

from lichess.util import printprogress, DATAROOT, FIELDS, DBNAME

NPROC = 1

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
    """ consume_newlines """
    nl_count = 0
    line = fptr.readline()
    while line and nl_count < nnl:
        line = fptr.readline()
        if line == b"\n":
            nl_count += 1


def read_til_break(fptr):
    """ read all lines into list until encounter newline
    """
    line = fptr.readline()
    txt = [line.strip()]
    while line and line != b'\n':
        line = fptr.readline()
        if not line:
            return []
        txt.append(line.strip())

    return [entry.decode() for entry in txt if entry]


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


def parse_insert_one_row(pgn, sql):
    """ parse_insert_one_row """

    parsed = read_til_break(pgn)
    moves = read_til_break(pgn)

    if len(parsed) == 0 or len(moves) == 0:
        return

    game = pgn2dict(parsed)
    game['Moves'] = moves[0]

    row = build_insert(game)
    if "?" in row:
        return True
    cmd = 'INSERT INTO games VALUES (%s)' % (row)
    sql.execute(cmd)
    return True


def process_one_file(pgn, sql):
    """ multiprocessing target
    orient to correct location in file, create a unique sql connection
    and iterate over games until done
    """

    # setup sql db
    print("worker: connecting to db %s..." % (DBNAME))

    # loop over games
    countr = itertools.count()
    while parse_insert_one_row(pgn, sql) is not None:
        cur = next(countr)
        if cur % 1000 == 0:
            printprogress(
                "%s: %d processed..."
                % (__file__, cur))

    print('complete')


def main():
    """ pgn2sql main
    """

    pgn_files = glob.glob(os.path.join(DATAROOT, '*bz2'))
    sql = sqlite3.connect(os.path.join(DATAROOT, DBNAME))
    cursor = sql.cursor()
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS games (%s)'
        % (', '.join([' '.join(column) for column in FIELDS])))

    for pgn_file in pgn_files:
        print("begin %s..." % (pgn_file))
        bzf = bz2.BZ2File(pgn_file)
        process_one_file(bzf, cursor)
        bzf.close()

    # add relevant indexing
    cursor.execute("CREATE INDEX idx_white_elo on games(WhiteElo)")
    cursor.execute("CREATE INDEX idx_black_elo on games(BlackElo)")

    # clean up
    sql.commit()
    sql.close()


if __name__ == "__main__":
    main()
