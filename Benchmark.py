#
# Benchmark
#
# Measure timings of the following tasks 
# * retrieve a thread with given ID
# * add a new post to a given thread
# * edit a given post
#

from MySQLControls import MySQLControls
from Neo4JControls import Neo4JControls
from MongoControls import MongoControls

from ReadXML import XMLReader
from random import choice
from MyTimer import Timer


from pprint import pprint as PRINT

DATASETS = [
     {
     "name" : "TexLatex",
     "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/users.xml",
     "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/posts.xml"
     },
     {
      "name":  "SuperUser",
      "USER_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/users.xml",
      "POST_XML": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/posts.xml"
    }
    ]

# Database Access Objects
DBOS = [
        MySQLControls(),
        # Neo4JControls(),
        MongoControls()       
    ]



def main():
    BO = Benchmark(DATASETS,DBOS)
    BO.run()

class Benchmark:
    def __init__(self, DATASETS, DBOS):
        self.DATASETS = DATASETS
        self.DBOS     = DBOS
    
    def run(self):
        for DATASET in self.DATASETS:
            print "==== Evaluating Dataset %s === " % DATASET["name"]
            XRO = XMLReader.from_dataset(DATASET)
            PRINT( XRO.get_stats() )
            
            for DBO in self.DBOS:
                print "==== Benchmarking %s ===" % DBO.info
                
                print "* reset tables", DBO.info
                DBO.reset()
                
                print "* import dataset"
                DBO.import_XML(XRO)
                
                print "* Benchmark Retrieval"
                self.retrieval_benchmark(DBO)
                
                #print "* Benchmark Add Post"
                self.add_post_benchmark(DBO)
                
                #print "* Benchmark Edit Post"
                self.edit_post_benchmark(DBO)
        
        for DBO in DBOS:
            DBO.close()
    
    def retrieval_benchmark(self,DBO, runs  = 1000):
        thread_ids = DBO.XRO.get_thread_ids()
        timer = Timer()
        timer.start()
        for i in range(runs):
            DBO.get_thread(choice(thread_ids))
            timer.tick()
        timer.stop()
        timer.show()
            
            
    def add_post_benchmark(self,DBO):
        pass
    
    def edit_post_benchmark(self,DBO):
        pass


if __name__ == "__main__": 
    main()