""" module for small utilities
"""

import os
import sys

DATAROOT = os.path.join("/mnt", "data", "chess")
URLROOT = "https://database.lichess.org/standard"
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
DBNAME = "lichess_games.sql"



def printprogress(msg):
    """ printprogress
    """
    # print("\033c")  # clear console
    sys.stdout.write("\r" + msg)
    sys.stdout.flush()
