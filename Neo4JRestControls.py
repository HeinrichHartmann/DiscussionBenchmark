import os
os.environ['NEO4J_PYTHON_JVMARGS'] = '-Xms512M -Xmx1024M'
os.environ['CLASSPATH'] = '/usr/lib/jvm/java-6-openjdk/jre/lib/'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-6-openjdk/jre/'

from TimeDec import TimeDec
import neo4jrestclient.client as neo4j 
# Docs https://neo4j-rest-client.readthedocs.org/en/latest/info.html#getting-started

from ReadXML import XMLReader
from WriteCSV import CSVWriter
from subprocess import call

NEO4J_BIN = "/home/heinrich/Programs/neo4j-community-1.8.1/bin/neo4j"
NEO4J_URL = "http://localhost:7474/db/data"
BATCH_IMPORT_JAR = "/home/heinrich/Programs/neo4j-batch-import/target/batch-import-jar-with-dependencies.jar"
DB_FOLDER = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/Neo4J"
LOG_FILE = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/Neo4J.log"

# WARNING!
# Make sure server is configuered to use DB_FOLDER!!
# edit: neo4j-community-1.8.1/conf/neo4j-server.properties


class Neo4JRestControls:
    info = "Neo4J REST"
    
    def __init__(self, url = NEO4J_URL):
        self.url = url
        self.server_start()

    #
    # Server contoling
    #
    def reset(self):
        print "Reset Neo4J db"
        self.index = None
        call("rm -Rf " + DB_FOLDER, shell = True)

        # self.db.query("""
        #    START n=node(*)
        #    MATCH n-[r?]-()
        #    WHERE ID(n) <> 0
        #    DELETE n,r
        #    """).get_response()

    def open(self):       
        self.db = neo4j.GraphDatabase(self.url)
        try:    
            self.index = self.db.node.indexes.get("threadID")
        except neo4j.NotFoundError:
            self.index = None
                     
        
    def close(self):
        self.index = None
        self.db = None
        self.server_stop()

    def server_stop(self):
        call(NEO4J_BIN + " stop", shell=True)
    
    def server_start(self):
        call(NEO4J_BIN + " start", shell=True)
        print "Please restart the Neo4jDB by hand if not succesfull!"
        print NEO4J_BIN + " start"
        raw_input()

        self.open()

    def server_restart(self):
        self.server_stop()
        self.server_start()
        
    def server_status(self):
        call(NEO4J_BIN + " status", shell=True)

    

    #
    # Bulk Import Methods
    #    
    def bulk_import(self, 
        nodes_csv     = "nodes.csv",
        relations_csv = "relations.csv",
        index_csv     = "thread_index.csv",
        index_name    = "threadID",
        db_folder     = DB_FOLDER,
        jar           = BATCH_IMPORT_JAR
        ):

        EXEC = "java -server -Xmx4G -jar {jar} {db_folder} {nodes_csv} {relations_csv} node_index {index_name} exact {index_csv}"
        call(EXEC.format(**locals()), shell = True)
        
    @TimeDec
    def import_XML(self, XRO):
        self.XRO = XRO
        name = XRO.name
        
        CWO = CSVWriter(XRO)

        print "* Wirte CSV"
        CWO.write_nodes(name + "nodes.csv")
        CWO.write_relations(name + "relations.csv")
        CWO.write_thread_index(name + "thread_index.csv")

        print "* Writing Neo4J"
        self.bulk_import(
             nodes_csv = name + "nodes.csv", 
             relations_csv = name + "relations.csv", 
             index_csv = name + "thread_index.csv"
             )
        
        self.server_restart()

    #
    # Access methods
    #

    def get_thread(self, ID):
        try:
            node  = self.index["ID"][ID].single
        except IndexError:
            print "threadID", ID, "not found"
            return

        records = []
        for sub_node in node.traverse(
            [
                # traverse edges of type CONTAINS and WRITTEN_BY 
                # starting from the thread node
                neo4j.Outgoing.CONTAINS,
                neo4j.Outgoing.WRITTEN_BY
            ],  
                # set max_depth of traversal to 2
                stop = 2                           
            ):

            records.append(sub_node.properties)
        
        return records


def TEST():
    DBO = Neo4JRestControls()    
    DBO.reset()
    DBO.import_XML(XMLReader())

    DBO.server_restart()
    DBO.get_thread(1)

if __name__ == "__main__":
    TEST()
    
    
    
    
    
    
    