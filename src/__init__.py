from termcolor import colored, cprint
from tqdm import tqdm
from bril import ExcelWriter, copy, createRandomSample
import bril as br
import datetime as dt
import pathlib as pl
from pprint import pprint as pp
import yaml
import pandas as pd
import paramiko
import os
import re
import sys

root = pl.Path(__file__).parents[1]
with open(root / "configs" / "config.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)