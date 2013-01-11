import csv
import re

from ReadXML import get_threads, get_posts, get_users

def main():
    print "writing users.cvs"
    write_users()
    print "writing threads.cvs"
    write_threads()
    print "writing posts.cvs"
    write_posts()

    print "writing nodes.cvs"
    write_nodes()
    print "writing relations.cvs"
    write_relations()

def write_users(file_name = "users.csv"):
    with open(file_name, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(
            ([user["ID"], esc(user["name"])] for user in get_users())              
            )
        
def write_threads(file_name = "threads.csv"):
    with open(file_name, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows( 
            (
             [ thread["ID"], esc(thread["title"]) ]
             for thread in get_threads())
             )


def write_posts(file_name = "posts.csv"):
    with open(file_name, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows((
             [post["ID"],
             post["threadID"], 
             post["userID"],
             esc(post["content"]),
             post["date"].replace("T"," ")[0:19]
             ]
             for post in get_posts())
             )
        
from collections import defaultdict

# dictionaries that return 2 if key not found
user_row = {}
thread_row = {}
post_row = {}


def write_nodes(file_name = "nodes.csv"):
    # generate cvs for Neo4J import via 
    # https://github.com/jexp/batch-import
    
    with open(file_name,'wb') as f:
        writer = csv.writer(f, delimiter = "\t")
        # first line lists all properties
        writer.writerow(["type", "ID", "name", "title", "content" , "date"])
        row_number = 1
        for user in get_users():
            writer.writerow(
            # ["type", "ID"      , "name"           , "title", "content", "date"]
              ["user", user["ID"], esc(user["name"]), ""     , ""       , ""]
              )
            
            user_row[user["ID"]] = row_number
            row_number += 1            
            
        for thread in get_threads():
            writer.writerow(
            # ["type"  , "ID"        , "name", "title"             , "content", "date"]
              ["thread", thread["ID"], ""    , esc(thread["title"]), ""       , ""    ]
              )
            
            thread_row[thread["ID"]] = row_number
            row_number += 1
        
        for post in get_posts():
            writer.writerow(
            # ["type", "ID"      , "name", "title", "content"           , "date"]
              ["post", post["ID"], ""    , ""     , esc(post["content"]),post["date"]]
              )
            
            post_row[post["ID"]] = row_number
            row_number += 1



def write_relations(file_name = "relations.csv"):
    # generate cvs for Neo4J import via 
    # https://github.com/jexp/batch-import
    
    with open(file_name,'wb') as f:
        writer = csv.writer(f, delimiter = "\t")
        writer.writerow(["start","end","type"])
        
        for post in get_posts():
            # edge from post to user
            try:
                writer.writerow([ 
                                 post_row[post["ID"]],
                                 user_row[post["userID"]],
                                 "WRITTEN_BY"
                              ])
            except KeyError:
                print "Row not found for user", post["userID"]
                continue
            
            # edge from thread to post
            try:
                writer.writerow([ thread_row[post["threadID"]], 
                              post_row[post["ID"]],
                              "CONTAINS"
                              ])
            except KeyError:
                print "Row not found for thread", post["threadID"]
                continue


def esc(s = ""):
#    s = re.sub("""[\n,\t"']""",'',s)
    s = re.sub("""[^A-Za-z0-9\ ]""",'',s)
    return s.encode('ascii','ignore')

if __name__ == "__main__": main()
