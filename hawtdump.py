import struct, os

def readStr (f):
  s = ''
  while True:
    c = f.read(1)
    if c == '\x00':
      return s

    s += c


def dumpExtent (f, startPage, pageSize, headerSize):

  def dumpSingleExtent (page):
    fileOffset = headerSize + (page * pageSize)
    f.seek(fileOffset)

    magic = readStr(f)
    f.seek(-1, os.SEEK_CUR)

    header = f.read(8)

    (extentLength, nextPage) = struct.unpack(">ii", header)

    data = f.read(extentLength - (len(magic) + 8))

    hex = lambda d: ' '.join(i.encode('hex') for i in d).upper()

    safestr = lambda d: ''.join(31 < ord(i) < 127 and i or '.' for i in d)

    print "Extent @ " + str(page) + ' 0x{0:08X}'.format(fileOffset)
    print "  magic:  " + hex(magic) + ' "' + safestr(magic) + '"'
    print "  length: " + str(extentLength)
    print "  next:   " + str(nextPage)



    for offset in range(0, len(data), 16):
      chunk = data[offset:offset+16]
      print('{0:08X} + {1:04X} | {2:47s} | {3:s}'.format(fileOffset, offset, hex(chunk), safestr(chunk)))

    return nextPage

  currentPage = startPage
  while currentPage != -1:
    currentPage = dumpSingleExtent(currentPage)


