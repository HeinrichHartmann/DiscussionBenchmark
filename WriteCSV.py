import csv
import re

from ReadXML import XMLReader

DEBUG = False

def main():
    XRO = XMLReader()
    
    CWO = CSVWriter(XRO)
    
    print "writing users.csv"
    CWO.write_users()
    print "writing threads.csv"
    CWO.write_threads()
    print "writing posts.csv"
    CWO.write_posts()

    print "writing nodes.csv"
    CWO.write_nodes()
    print "writing relations.csv"
    CWO.write_relations()
    print "writing thread_index.csv"
    CWO.write_thread_index()

class CSVWriter:
    
    def __init__(self, XMLReaderObject = XMLReader() ):
        self.XRO = XMLReaderObject
    

    
    def write_users(self, out_file =  "users.csv"):
        with open(out_file, 'wb') as f:
            writer = csv.writer(f)
            writer.writerows(
                ([user["ID"], esc(user["name"])] for user in self.XRO.get_users())              
                )
            
    def write_threads(self, out_file =  "threads.csv"):
        with open(out_file, 'wb') as f:
            writer = csv.writer(f)
            writer.writerows( 
                (
                 [ thread["ID"], esc(thread["title"]) ]
                 for thread in self.XRO.get_threads())
                 )
    
    
    def write_posts(self, out_file = "posts.csv"):
        with open(out_file, 'wb') as f:
            writer = csv.writer(f)
            writer.writerows(([
                post["ID"],
                post["threadID"], 
                post["userID"],
                esc(post["content"]),
                post["date"].replace("T"," ")[0:19]
                ]
                for post in self.XRO.get_posts())
                )


    user_row = {}
    thread_row = {}
    post_row = {}    
    def write_nodes(self, out_file = "nodes.csv"):
        # generate cvs for Neo4J import via 
        # https://github.com/jexp/batch-import
        
        with open(out_file,'wb') as f:
            writer = csv.writer(f, delimiter = "\t")
            # first line lists all properties
            writer.writerow(["type", "ID", "name", "title", "content" , "date"])
            row_number = 1
            for user in self.XRO.get_users():
                writer.writerow(
                # ["type", "ID"      , "name"           , "title", "content", "date"]
                  ["user", user["ID"], esc(user["name"]), ""     , ""       , ""]
                  )
                
                self.user_row[user["ID"]] = row_number
                row_number += 1            
                
            for thread in self.XRO.get_threads():
                writer.writerow(
                # ["type"  , "ID"        , "name", "title"             , "content", "date"]
                  ["thread", thread["ID"], ""    , esc(thread["title"]), ""       , ""    ]
                  )
                
                self.thread_row[thread["ID"]] = row_number
                row_number += 1
            
            for post in self.XRO.get_posts():
                writer.writerow(
                # ["type", "ID"      , "name", "title", "content"           , "date"]
                  ["post", post["ID"], ""    , ""     , esc(post["content"]),post["date"]]
                  )
                
                self.post_row[post["ID"]] = row_number
                row_number += 1
    
    
    def write_relations(self, out_file = "relations.csv"):
        # generate cvs for Neo4J import via 
        # https://github.com/jexp/batch-import
        
        with open(out_file,'wb') as f:
            writer = csv.writer(f, delimiter = "\t")
            writer.writerow(["start","end","type"])
            
            for post in self.XRO.get_posts():
                # edge from post to user
                try:
                    writer.writerow([ 
                         self.post_row[post["ID"]],
                         self.user_row[post["userID"]],
                         "WRITTEN_BY"
                    ])
                except KeyError:
                    if DEBUG: print "Row not found for user", post["userID"]
                    continue
                
                # edge from thread to post
                try:
                    writer.writerow([ 
                        self.thread_row[post["threadID"]], 
                        self.post_row[post["ID"]],
                        "CONTAINS"
                    ])
                except KeyError:
                    if DEBUG: print "Row not found for thread", post["threadID"]
                    continue
    
    def write_thread_index(self,out_file = "thread_index.csv"):
        with open(out_file,'wb') as f:
            writer = csv.writer(f, delimiter = "\t")
            writer.writerow(["id","ID"])
            for threadID, row in self.thread_row.items():
                writer.writerow([row, threadID])
    

def esc(s = ""):
#    s = re.sub("""[\n,\t"']""",'',s)
    s = re.sub("""[^A-Za-z0-9\ ]""",'',s)
    return s.encode('ascii','ignore')

if __name__ == "__main__": main()
