from time import time
import csv
import json

LOGFILE = "time.log"

class Timer:
    """
    Class for time mesurements.
    """
    title = ""
    timeStart = 0.0
    timeStop = 0.0
    timeLast = 0.0
    ticks  = []

    logfile = ""

    def __init__(self,title = "Timer", logfile = LOGFILE):
        self.title = title
        self.ticks = []
        self.logfile = logfile
    
    def start(self):
        #print "starting clock " + self.title
        self.timeStart = time()
        self.timeLast = self.timeStart

    def tick(self):
        timeNow = time()
        self.ticks.append(timeNow - self.timeLast)
        self.timeLast = timeNow

    def stop(self):
        #print "stopping clock " + self.title
        self.timeStop = self.timeLast

   
    def show(self):
        title = self.title
        timeTotal = self.timeStop - self.timeStart
        nTicks = len(self.ticks)
        timeAverage = 1000 * timeTotal / float(nTicks)
        timeMax = 1000 * max(self.ticks)
        timeMin = 1000 * min(self.ticks)
        frequency = nTicks / float(timeTotal)
        fMax = 1.0 / (min(self.ticks) + 0.000001)
        fMin = 1.0 / (max(self.ticks) + 0.000001)

        print """\
Perfoming {title}:
* Ticks:     {nTicks} 
* Total:     {timeTotal:.3f}sec
* Frequency: {frequency:.3f} ticks/sec ({fMin:.3f} - {fMax:.3f})
* Average:   {timeAverage:.3f} msec/ticks ({timeMin:.3f} - {timeMax:.3f})
""".format(**locals())

        