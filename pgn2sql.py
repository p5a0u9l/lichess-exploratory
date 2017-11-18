""" convert a file of pgn chess games to a sqlite3 database
"""

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
    """ consume_newlines """
    nl_count = 0
    line = fptr.readline()
    while line and nl_count < nnl:
        line = fptr.readline()
        if line == "\n":
            nl_count += 1


def read_til_break(fptr):
    """ read all lines into list until encounter newline
    """
    line = fptr.readline()
    txt = [line.strip()]
    while line and line != '\n':
        line = fptr.readline()
        if not line:
            return False
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


def parse_insert_one_row(cursor, pgn):
    """ parse_insert_one_row """

    parsed = read_til_break(pgn)
    if not isinstance(parsed, list):
        return False
    game = pgn2dict(parsed)
    game['Moves'] = read_til_break(pgn)[0]

    row = build_insert(game)
    if "?" in row:
        return True
    cmd = 'INSERT INTO games VALUES (%s)' % (row)
    cursor.execute(cmd)
    return True


def worker(pgn, worker_index):
    """ multiprocessing target
    orient to correct location in file, create a unique sql connection
    and iterate over games until done
    """

    # setup sql db
    dbname = sys.argv[1].replace('.pgn', '-worker%0.2d.sql' % (worker_index))
    print("worker%0.2d: connecting to db %s..." % (worker_index, dbname))
    sql = sqlite3.connect(dbname)
    cursor = sql.cursor()
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS games (%s)'
        % (', '.join([' '.join(column) for column in FIELDS])))

    # loop over games
    i = 0
    while True:
        i += 1
        if i % 1000 == 0:
            printprogress(
                "%s worker %0.2d: %d processed..."
                % (__file__, worker_index, i))

        result = parse_insert_one_row(cursor, pgn)

        if not result:
            break

    print('complete')
    # clean up
    sql.commit()
    sql.close()


def count_games(fptr):
    """ count_games """
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


def main():
    """ pgn2sql main
    """

    pgn_file = open(sys.argv[1])

    # create multiple processes
    if NPROC > 1:
        procs = [
            multiprocessing.Process(
                target=worker,
                args=(pgn_file, idx))
            for idx in range(NPROC)]

        # start the processes and monitor
        for proc in procs:
            proc.start()

        while any([proc.is_alive() for proc in procs]):
            time.sleep(1)
    else:
        worker(pgn_file, 0)

    pgn_file.close()

if __name__ == "__main__":
    main()
