import os
os.environ['NEO4J_PYTHON_JVMARGS'] = '-Xms512M -Xmx1024M'
os.environ['CLASSPATH'] = '/usr/lib/jvm/java-6-openjdk/jre/lib/'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-6-openjdk/jre/'

from TimeDec import TimeDec
import neo4j

from ReadXML import XMLReader
from WriteCSV import CSVWriter
from subprocess import call

BATCH_IMPORT_JAR = "/home/heinrich/Programs/neo4j-batch-import/target/batch-import-jar-with-dependencies.jar"
DB_FOLDER = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/NativeNeo4J"
LOG_FILE = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/Neo4J.log"


class Neo4JNativeControls:
    info = "Neo4J Native"
    path = DB_FOLDER

    def __init__(self, path = DB_FOLDER):
        self.path = path
        self.open()
        
    #
    # Server contoling
    #
    def reset(self):
        print "Reset Neo4J db"
        self.close()
        call("rm -Rf " + DB_FOLDER, shell = True)
        self.open()

        # self.db.query("""
        #    START n=node(*)
        #    MATCH n-[r?]-()
        #    WHERE ID(n) <> 0
        #    DELETE n,r
        #    """).get_response()
        

    def close(self):
        self.index = None
        self.db.shutdown()

    def open(self):
        self.db = neo4j.GraphDatabase(self.path)
        

    def restart(self):
        self.close()
        self.open()
    

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

        self.close()

        print "* Writing Neo4J"
        self.bulk_import(
             nodes_csv = name + "nodes.csv", 
             relations_csv = name + "relations.csv", 
             index_csv = name + "thread_index.csv"
             )
        
        self.open()

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
        try:
            node  = index.get("ID",ID).single
        except IndexError:
            print "threadID", ID, "not found"
            return

        records = []
        #traverser = self.db.traversal()\
        #        .relationships("CONTAINS", neo4j.OUTGOING)\
        #        .relationships("WRITTEN_BY", neo4j.OUTGOING)\
        #        .evaluator(stop_at_depth(2))\
        #        .traverse(node)
        
        result = self.db.query("""
            START t = node({start_id}) 
            MATCH (t) --> (p) --> (u)
            RETURN t,p,u
            """,start_id = node.getId())
                
        for row in result:
            for k,v in row['t'].items():
                records.append([k,v])
            for k,v in row['p'].items():
                records.append([k,v])
            for k,v in row['u'].items():
                records.append([k,v])
        
        return records
        
        

def TEST():
    DBO = Neo4JNativeControls()    
    DBO.reset()
    DBO.import_XML(XMLReader())
    print DBO.get_thread(1)

if __name__ == "__main__":
    TEST()
    
    
    
    
    
    
    