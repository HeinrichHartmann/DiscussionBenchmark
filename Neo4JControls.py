from neo4jrestclient.client import GraphDatabase
# Docs https://neo4j-rest-client.readthedocs.org/en/latest/info.html#getting-started

from ReadXML import XMLReader
from WriteCSV import CSVWriter
from subprocess import call

NEO4J_BIN = "/home/heinrich/Programs/neo4j-community-1.8.1/bin/neo4j"
BATCH_IMPORT_JAR = "/home/heinrich/Programs/neo4j-batch-import/target/batch-import-jar-with-dependencies.jar"
DB_FOLDER = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/Neo4J"
LOG_FILE = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/Neo4J.log"

# WARNING!
# Make sure server is configuered to use DB_FOLDER!!
# edit: neo4j-community-1.8.1/conf/neo4j-server.properties

class Neo4JControls:
    info = "Neo4J"
    def __init__(self, url = "http://localhost:7474/db/data/"):
        self.start_server()
        self.db = GraphDatabase(url)         

    #
    # Server contoling
    #
    def reset(self):
        print "Reset Neo4J db"
        #self.stop_server()
        call("rm -R " + DB_FOLDER, shell = True)
        #self.start_server()

    def close(self):
        self.stop_server()

    def stop_server(self):
        call(NEO4J_BIN + " stop >> " + LOG_FILE, shell=True)
    
    def start_server(self):
        call(NEO4J_BIN + " start >> " + LOG_FILE, shell=True)

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
        #self.stop_server()       
        EXEC = "java -server -Xmx4G -jar {jar} {db_folder} {nodes_csv} {relations_csv} node_index {index_name} exact {index_csv}"
        call(EXEC.format(**locals()), shell = True)
        #self.start_server()
        
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

    #
    # Access methods
    #
    index = None
    def get_index(self):
        if self.index: return self.index
        self.index = self.db.node.indexes.get("threadID")
        return self.index
        
    def get_thread(self, ID):
        index = self.get_index()
        for node in index["ID"][ID]:
            print node.items()

        
    def create_node(self, data = {}):
        # creates node with properties data
        # returns url of created node

        with self.db.transaction:
            node = self.db.node(data)

        return node
    
    def create_relation(self, source, target, relation_type = "", data = {}):
        rel = self.db.relationships.create(source, relation_type, target)
        
        for k,v in data.items():
            rel[k] = v
        
        return rel

def TEST():
    DBO = Neo4JControls()    
    DBO.reset()
    DBO.import_XML(XMLReader())

    DBO.server_status()
        
    DBO.get_thread(1)
    DBO.get_thread(2)
    DBO.get_thread(3)

if __name__ == "__main__":
    TEST()
    
    
    
    
    
    
    