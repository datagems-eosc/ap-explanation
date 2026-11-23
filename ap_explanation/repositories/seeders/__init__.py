"""
Setting a database up for a data source, for the time of a task: ``seed_database``.
Each data source needing a setup registers its seeder in a module of its own.
"""
# Imported for its registration: a seeder is only known once its module is loaded
from . import csv as csv
from .seed import seed_database

__all__ = ["seed_database"]
