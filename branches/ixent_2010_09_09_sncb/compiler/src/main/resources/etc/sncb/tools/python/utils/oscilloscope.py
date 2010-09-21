#!/usr/bin/env python

import sys
import tos

AM_OSCILLOSCOPE = 0x93

class OscilloscopeMsg(tos.Packet):
    def __init__(self, packet = None):
        tos.Packet.__init__(self,
                            [('id',       'int', 2),
                             ('readings', 'blob', None)],
                            packet)

if '-h' in sys.argv:
    print "Usage:", sys.argv[0], "serial@/dev/ttyUSB0:57600"
    sys.exit()

am = tos.AM()

topology = open('topology.txt', 'w')
resource = open('resource.txt', 'w')

while True:
    p = am.read()
    if p and p.type == AM_OSCILLOSCOPE:
        msg = OscilloscopeMsg(p.data)
#        print msg.id, [i<<8 | j for (i,j) in zip(msg.readings[::2], msg.readings[1::2])]
        zipped = zip(msg.readings[::2], msg.readings[1::2])
        for i in range(0, len(zipped), 2):
            dest = zipped[i][0] << 8 | zipped[i][1]
            energy = zipped[i+1][0] << 8 | zipped[i+1][1]
            if dest != 65535:
                topology.write('Link: source=\"' + str(msg.id) + '\" dest=\"' + str(dest) + '\" bidirectional=\"true\" energy=\"' + str(energy) + '\" time=\"0\" radio-loss=\"0\"\n')
                resource.write('Link: id=\"' + str(msg.id) + '\" energy=\"9999\" ram=\"10\" flash=\"48\"\n')
