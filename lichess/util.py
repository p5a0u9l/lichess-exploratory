""" module for small utilities
"""

import os
import sys
import re

DATAROOT = os.path.join("/mnt", "data", "chess")
DBNAME = "lichess_games.sql"
URLROOT = "https://database.lichess.org/standard"
TESTPGNNAME = "tester.pgn.bz2"
TESTDBNAME = "test_lichess_games.sql"
PLAYER_FIELDS = [
    ('Handle', 'text'),
    ('N_Game', 'real'),
    ('AvgElo', 'real')
]

GAME_FIELDS = [
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
MINELO = 800
MAXELO = 2900
QUIET = False


def printprogress(msg):
    """ printprogress
    """
    # print("\033c")  # clear console
    sys.stdout.write("\r" + msg)
    sys.stdout.flush()


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


def player_games(sql_db, player):
    """ player_games """
    pass
