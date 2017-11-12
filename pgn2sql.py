#!/usr/bin/env python

import re
import sys
import sqlite3

import bpdb

GAMELENGTH = 100
SQL = sqlite3.connection(sys.argv[1].replace('.pgn', '.sql'))


def pgn2dict(txt):
    result = {}
    for line in txt:
        if not line:
            continue
        match = re.search(r'(\w+) "(.*)"', line).groups()
        result[match[0]] = match[1]

    return result


def read_til_break(fptr):
    line = fptr.readline()
    txt = [line.strip()]
    while line != '\n':
        line = fptr.readline()
        txt.append(line.strip())

    return [entry for entry in txt if entry]


def game2sql(game):
    for key, val in game.items():


def main(png):

    games = []

    while len(games) < GAMELENGTH:
        game = pgn2dict(read_til_break(png))
        game['Moves'] = read_til_break(png)[0]
        games.append(game)

    bpdb.set_trace()


if __name__ == "__main__":
    png = open(sys.argv[1])
    main(png)
    png.close()
