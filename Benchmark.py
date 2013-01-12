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



DATASETS = [
     {
     "name" : "Tex/Latex",
     "USER_XML_file": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/users.xml",
     "POST_XML_file": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/posts.xml"
     },
     {
    "name":  "SuperUser",
    "USER_XML_file": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/users.xml",
    "POST_XML_file": "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/posts.xml"
    }
]

# Database Access Objects
DBOS = [
        MySQLControls(),
        Neo4JControls(),
        MongoControls()       
        ]

def main():
    for DATASET in DATASETS:
        print "Evaluating Dataset %s" % DATASET[name]
        prepare_data(DATASET)
        
        for DBO in DBOS:
            print " ==== Benchmarking %s === " % DBO.info
            
            print "* reset tables", DBO.info
            DBO.reset()
            
            print "* import dataset"
            DBO.import_dataset(DATASET)
            
            print "* Benchmark Retrieval"
            retrieval_benchmark(DBO, DATASET)
            
            #print "* Benchmark Add Post"
            add_post_benchmark(DBO, DATASET)
            
            #print "* Benchmark Edit Post"
            edit_post_benchmark(DBO, DATASET)

def retrieval_benchmark(DBO, DATASET):
    pass
   
def add_post_benchmark(DBO, DATASET):
    pass

def edit_post_benchmark(DBO, DATASET):

def prepare_data(DATASET):
    # extract csv
    # add basic statistics
    pass

def get_stats(DATASET):
    return DATASET["name"]


if __name__ == "__main__": 
    main()