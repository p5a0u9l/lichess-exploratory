""" download/verify
"""

import os
import time
import subprocess

from lichess.util import DATAROOT, URLROOT


def main():
    """ main """
    with open(os.path.join(DATAROOT, "sha256sums.txt")) as fptr:
        os.chdir(DATAROOT)
        for line in fptr.readlines():
            dbname = line.split()[1]
            print("start download of %s at %s..." % (dbname, time.ctime()))
            subprocess.call(["wget", os.path.join(URLROOT, dbname)])

    subprocess.call(
        ["sha256sum", "-c",
         os.path.join(DATAROOT, "sha256sums.txt")])

if __name__ == "__main__":
    main()
