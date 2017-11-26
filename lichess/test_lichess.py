""" test module for slice_db
"""

import unittest
import os

from lichess import util

util.QUIET = True


class TestSqlIo(unittest.TestCase):
    """ TestSqlIo """

    def setUp(self):
        """ setUp """
        from lichess import sql_io
        self.cdb = sql_io.ChessDB(dbname=util.TESTDBNAME)

    def test_execute(self):
        """ test_execute """
        self.cdb.execute(
            'CREATE TABLE IF NOT EXISTS test (%s)'
            % (', '.join([' '.join(column) for column in util.GAME_FIELDS])))

    def test_fetchone(self):
        """ test_fetchone """
        pass


class TestCreateDb(unittest.TestCase):
    """ TestCreateDb """

    def setUp(self):
        """ setUp """
        from lichess import sql_io
        self.cdb = sql_io.ChessDB(dbname=util.TESTDBNAME)

    def test_create_games_indices(self):
        """ test_create_games_indices """
        from lichess import create_db
        create_db.create_games_table(self.cdb, pgn_glob=util.TESTPGNNAME)
        create_db.create_games_indices(self.cdb)
        self.cdb.cleanup()

    def test_create_players_table(self):
        """ test_create_players_table """
        from lichess import create_db
        create_db.create_games_table(self.cdb, pgn_glob=util.TESTPGNNAME)
        create_db.create_games_indices(self.cdb)
        create_db.create_players_table(self.cdb)
        self.cdb.cleanup()

    def test_create_games_table(self):
        """ test_create_games_table """
        from lichess import create_db
        create_db.create_games_table(self.cdb, pgn_glob=util.TESTPGNNAME)
        self.cdb.cleanup()


class TestSliceDb(unittest.TestCase):
    """ TestSliceDb """

    def setUp(self):
        """ setUp """
        from lichess import sql_io
        self.cdb = sql_io.ChessDB(dbname=util.TESTDBNAME)

    def test_plot_hist_elo_games(self):
        """ test_plot_hist_elo_games """
        from lichess import slice_db
        slice_db.plot_hist_elo_games(self.cdb, "test.png")

        self.assertTrue(
            os.path.exists(os.path.join(util.DATAROOT, "plots", "test.png")))

    def test_plot_hist_name_players(self):
        """ test_plot_hist_elo_games """
        from lichess import slice_db
        slice_db.plot_hist_name_players(self.cdb, "test.png")

        self.assertTrue(
            os.path.exists(os.path.join(util.DATAROOT, "plots", "test.png")))


if __name__ == "__main__":
    os.remove(os.path.join(util.DATAROOT, util.TESTDBNAME))
    unittest.main(verbosity=3)
