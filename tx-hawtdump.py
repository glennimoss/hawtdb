#!/usr/bin/python

import sys, hawtdump

filename = sys.argv[1]
page = int(sys.argv[2])

f = open(filename, "rb")
headerMagic = readStr(f)
f.seek(40)
pageSize = struct.unpack(">i", f.read(4))[0]

print '{0:s} pageSize = 0x{1:X}'.format(headerMagic, pageSize)

hawtdump.dumpExtent(f, page, pageSize, 0x1000)

f.close()
