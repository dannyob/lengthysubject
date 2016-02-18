#!/usr/bin/env python
##
# strip_ids.py
###
"""strip_ids.py

Our email subject line length database is pretty skimpy on personal information,
but the message-ids that are used as a primary key (to prevent duplication) can
reveal info about the sender or the recipient of the email, including hostname,
timezone, OS, mail client, etc.

This turns that index into a simple integer that has no relation to the original
message-id, then uses the SQLITE command VACUUM to remove the previous data.

The resulting table can still be used for subject line length analysis.

"""

__author__ = "Danny O'Brien <http://www.spesh.com/danny/>"
__copyright__ = "Copyright Danny O'Brien"
__contributors__ = None
__license__ = "GPL v3"

import sqlite3

c = sqlite3.connect("emailsubjectlinelengths.db")

c.execute('CREATE TABLE copy(id TEXT PRIMARY KEY, date TEXT, subject INT);')
c.execute('INSERT INTO copy SELECT rowid, date, subject FROM email_stats;')
c.execute('ALTER TABLE email_stats RENAME TO old_stats;')
c.execute('ALTER TABLE copy RENAME TO email_stats;')
c.execute('DROP TABLE old_stats;')
c.execute('VACUUM;')
