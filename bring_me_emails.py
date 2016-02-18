#!/usr/bin/env python
##
# bring_me_emails.py
###
"""bring_me_emails

It seemed like email subject lines have been getting longer, and I wanted to
create a database of the subject line lengths in all my emails.

Part of my historical email archive is stored as (gzipped) mbox files (see
<URI:https://en.wikipedia.org/wiki/Mbox>. The rest are stored in a notmuchmail
database (see <URI:https://notmuchmail.org/>.

This program has three functions that yield a tuple of message-id, subject, and
date headers for mbox directories, a notmuch database, and for completeness, a
Maildir folder.

These are chained together to feed to a main function that stores these values
in a sqlite3 database for later analysis.

The schema for this database is an unique id field (uses the message-id, but can
be cleaned using strip_ids.py , a date field, and the length of the subject line
as an int.

Analysis takes place elsewhere (for example, in this ipython notebook: Subject
line growth-sqlite-scatter.ipynb )

"""

__author__ = "Danny O'Brien <http://www.spesh.com/danny/>"
__copyright__ = "Copyright Danny O'Brien"
__contributors__ = None
__license__ = "GPL v3"

import os
import sys
import gzip
import mailbox
import email.utils
import datetime
import logging

try:
    import notmuch
except ImportError:
    pass

import sqlite3

from itertools import chain

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('bring_me_emails')

try:
    import coloredlogs
    coloredlogs.install(level=logging.DEBUG, logger=log)
except ImportError:
    pass

conn = sqlite3.connect('emailsubjectlinelengths.db')
conn.execute("create table if not exists email_stats(id TEXT PRIMARY KEY, date TEXT, subject INT)")
c = conn.cursor()


def bring_me_mboxen(path):
    """ Walks down a directory looking for mboxes, gzipped and uncompressed.
        Only feeds on files ending in '.mbx' or '.mbox' or '.mbx.gz'.

        Yields (message-id, subject, date) header strings """

    for root, dirs, files in os.walk(path):
        j = sorted([os.path.join(root, i) for i in files if i.endswith(".mbox") or i.endswith(".mbx") or i.endswith(".mbx.gz")])
        if j == []:
            continue
        for i in j:
            if i.endswith(".gz"):
                mailbox.open = gzip.open
            else:
                mailbox.open = open
            m = mailbox.mbox(i)
            log.debug("Scanning: {}".format(i))
            for em in m:
                yield (em['message-id'], em['subject'], em['date'])


def bring_me_a_maildir(path):
        m = mailbox.Maildir(path, factory=None)
        log.debug("Scanning maildir: {}".format(path))
        for em in m:
                print em['message-id']
                yield (em['message-id'], em['subject'], em['date'])


def bring_me_notmuchmail(path):
    """ Scans all emails from a notmuchmail database.

        Yields (message-id, subject, date) header strings """

    db = notmuch.Database(path)
    query = db.create_query('date:..'+str(datetime.datetime.now())[:10])  # until today
    total = query.count_messages()
    log.debug("Scanning notmuch: {} mails".format(total))
    j = query.search_messages()
    for l in j:
        mid = l.get_message_id()
        dd = l.get_header('date')
        sub = l.get_header('subject')
        if (not mid.startswith('<')):
            mid = '<' + mid + '>'
        yield (mid, sub, dd)


notmuch_path = '/home/mailuser/mynotmuchpath'
mailbox_path = '/home/mailuser/mymailboxesarehere'
maildir_path = '/home/mailuser/a_single_Maildir_folder/'
all_mails = chain(bring_me_mboxen(mailbox_path), bring_me_a_maildir(maildir_path), bring_me_notmuchmail(notmuch_path))

cnt = 0
earliest = '2038-01-01'
latest = '1970-01-01'

# main email -> database storage routine
for (mail_id, mail_subject, mail_date) in all_mails:
    try:
        if ((mail_id is None) or (mail_subject is None) or (mail_date is None)):
            continue
        # returns a struct_time 9-tuple, last 3 values are useless though
        date_s = email.utils.parsedate(mail_date)
        if date_s is None:
            continue
        # convert the useful first 6 entries in struct_time into a
        # datetime object, stringify, extract out first 10 chars to get YYYY-MM-DD
        day = str(datetime.datetime(*date_s[:6]))[:10]
        if (day < '1990-01-01' or day > '2020-01-01'):  # FIXME simple outlier remover.
            continue

        # maintain a record of the current range, for display purposes
        if (day < earliest):
            earliest = day
        if (day > latest):
            latest = day

        # normalize message-id storage to '<mailid@example.com>'
        if (not mail_id.startswith('<')):
            mail_id = '<' + mail_id + '>'
        subject_len = len(mail_subject)

        # display and commit SQL transaction every 1000 emails
        cnt += 1
        if (0 == cnt % 1000):
            log.info("{} emails scanned, from {}..{}. Committing.".format(cnt, earliest, latest))
            conn.commit()
        c.execute("INSERT OR IGNORE INTO email_stats VALUES (?, ?, ?)", (mail_id, day, subject_len))
    except:
        print "Happily ignoring error:", sys.exc_info()
