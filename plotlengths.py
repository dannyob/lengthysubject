#!/usr/bin/env python
##
# plotlengths.py
###
"""plotlengths.py

"""

__author__ = "Danny O'Brien <http://www.spesh.com/danny/>"
__copyright__ = "Copyright Danny O'Brien"
__contributors__ = None
__license__ = "GPL v3"


import sys
import datetime

import numpy
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

import sqlite3

c = sqlite3.connect('emailsubjectlinelengths.db')
results = c.execute('select strftime("%Y-%m-%d", date), count(*), avg(subject) from email_stats group by strftime("%Y-%m-%d", date);')

x = []
y = []
c = []
for row in results:
    (x1, cnt, y1) = row
    x1d = datetime.date(int(x1[:4]),int(x1[5:7]), int(x1[8:]))
    # some sanity checking -- results still hold without these constraints, tho
    if x1 < '1999-01' or x1 > '2016-02-16' : # misdated emails outside of range
        continue
    if cnt > 4000:
        continue
    c.append(cnt)
    x.append(x1d)
    y.append(float(y1))
    
xx = mdates.date2num(x)

fig, ax1 = plt.subplots(figsize=(8,8))
ax2=ax1.twinx()


ax2.bar(x,c,color="0.6",edgecolor="0.6")
ax2.set_ylabel("Emails per day", color="0.5")

ax1.scatter(x,y, s=1, color="b")
z = numpy.polyfit(xx, y, 1)
p = numpy.poly1d(z)
ax1.set_ylabel("Length of subject line (chars)", color="b")
ax1.plot(xx,p(xx),"r-")

plt.savefig('result.png')
