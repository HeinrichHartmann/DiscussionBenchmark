#
# Benchmark
#
# Measure timings of the following tasks 
# * retrieve a thread with given ID
# * add a new post to a given thread
# * edit a given post
#

from MySQLControls import MySQLControls
from Neo4JNativeControls import Neo4JNativeControls
from Neo4JRestControls import Neo4JRestControls
from MongoControls import MongoControls

from ReadXML import XMLReader
from random import choice
from MyTimer import Timer

import csv

from pprint import pprint as PRINT

# Dataset list
DATASETS = [
#    {
#      "name":  "StackOverflow 1K",
#      "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/users.xml",
#      "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/posts.xml",
#      "max"     : 1000,
#     },
#    {
#      "name":  "StackOverflow 10k",
#      "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/users.xml",
#      "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/posts.xml",
#      "max"     : 10000
#     },
#    {
#      "name":  "StackOverflow 100K",
#      "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/users.xml",
#      "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/posts.xml",
#      "max"     : 100000
#     },
#    {
#      "name":  "StackOverflow 1M",
#      "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/users.xml",
#      "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Stack Overflow/posts.xml",
#      "max"     : 1000000
#     },
#    {
#     "name" : "Android",
#     "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Android/users.xml",
#     "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Android/posts.xml"
#     },
#    {
#     "name" : "unix",
#     "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Unix/users.xml",
#     "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Unix/posts.xml"
#     },
#     {
#     "name" : "TexLatex",
#     "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/users.xml",
#     "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/posts.xml"
#     },
#    {
#     "name" : "Ubuntu",
#     "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Ubuntu/users.xml",
#     "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Ubuntu/posts.xml"
#     },
#    {
#     "name" : "ServerFault",
#     "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 ServerFault/users.xml",
#     "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 ServerFault/posts.xml"
#     },
     {
      "name":  "SuperUser",
      "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/users.xml",
      "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/posts.xml"
    },
    ]

# Database Access Objects
DBOS = [
        Neo4JRestControls(),
        Neo4JNativeControls(),
        MySQLControls(),
        MongoControls()
    ]

# Number of runs
RUNS = 1000

# Logfile
LOGFILE = "NewBenchmarkResults.csv"
LOGCSV = csv.writer(open(LOGFILE, "a"))
LOGCSV.writerow(["Title", "DB","Dataset"])

# ThreadListFile 

THREADFILE = "threads.csv"



def main():
    BO = Benchmark(DATASETS,DBOS)
    BO.run()

class Benchmark:
    def __init__(self, DATASETS, DBOS):
        self.DATASETS = DATASETS
        self.DBOS     = DBOS
    
    def run(self):
        for DATASET in self.DATASETS:
            print "==== Importing Dataset %s === " % DATASET["name"]
            XRO = XMLReader.from_dataset(DATASET)
            stats = XRO.get_stats()
            PRINT( stats )
            LOGCSV.writerow(["Dataset Stats Keys", "", XRO.name] + stats.keys())
            LOGCSV.writerow(["Dataset Stats Values", "", XRO.name] + stats.values())
            
            for DBO in self.DBOS:                
                print "Current Db: ", DBO.info
                print "* reset tables"
                DBO.reset()
                
                print "* import dataset"
                DBO.import_XML(XRO)

            print "==== Retrieval Benchmark ===="
            print "* Generating samples"            
            warmup_sample = XRO.get_thread_sample(RUNS)
            thread_sample = XRO.get_thread_sample(RUNS)

            self.LOG_THREADS(thread_sample)

            LOGCSV.writerow(["Thread Lengths", "", XRO.name] +  [ XRO.get_thread_length(thread) for thread in thread_sample] )

            print "* Average Thread Length:", sum([ XRO.get_thread_length(thread) for thread in thread_sample])/float(RUNS)

            for DBO in self.DBOS:
                print "* Current Db: ", DBO.info
                # Warmup run
                print "* warming up caches"
                self.retrieval_benchmark(DBO, warmup_sample, silent = True)
                
                # Benchmark
                results = self.retrieval_benchmark(DBO, thread_sample)
                LOGCSV.writerow(["Retrieval Ticks", DBO.info, DBO.XRO.name] +  results )

            print "=== Edit Post Benchmark ==="
            print "* Generating samples"
            post_sample   = XRO.get_post_sample(1000)
            for DBO in self.DBOS:
                self.edit_post_benchmark(DBO, post_sample)
                
            #print "* Benchmark Add Post"
            #self.add_post_benchmark(DBO)
                
        
        for DBO in DBOS:
            DBO.close()
    
    def retrieval_benchmark(self,DBO, samples, silent = False):
        timer = Timer()
        timer.start()
        for sample in samples:
            DBO.get_thread(sample)
            #PRINT(DBO.get_thread(sample))
            #raw_input()
            timer.tick()
        timer.stop()
        if silent == False: timer.show()
        return timer.ticks
                
    def add_post_benchmark(self,DBO):
        pass
    
    def edit_post_benchmark(self,DBO, samples):
        pass

    def LOG_THREADS(self, threads):
        fh = open(THREADFILE, "w")
        TW = csv.writer(fh)
        TW.writerow(threads)
        fh.close()
        

if __name__ == "__main__": 
    main()