#!/usr/bin/python

import sys, hawtdump

filename = sys.argv[1]
pageSize = int(sys.argv[2])
page = int(sys.argv[3])

f = open(filename, "rb")

hawtdump.dumpExtent(f, page, pageSize, 0)

f.close()
