from lxml import etree
from pprint import pprint as PRINT

#USER_XML_file = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/users.xml"
#POST_XML_file = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/posts.xml"

USER_XML_file = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/users.xml"
POST_XML_file = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/posts.xml"

#
# Create Generators
#

def get_users(file = USER_XML_file):
    xml = etree.parse(open(file))
    
    yield {"ID":-1, "name": "Annonymous"}
    
    for row in xml.findall("row"):
        user_rec = {}

        user_rec["ID"] = int(row.get("Id"))
        user_rec["name"] = row.get("DisplayName")
        if None in user_rec.values(): 
            print "skipping user", user_rec["ID"]
            continue

        yield user_rec


def get_posts(file = POST_XML_file):
    xml = etree.parse(open(file))
    
    for row in xml.findall("row"):
        post_rec = {}

        post_rec["ID"] =      int(row.get("Id"))
        post_rec["userID"] =  int(row.get("OwnerUserId")) if row.get("OwnerUserId") else -1
        post_rec["content"] = row.get("Body")
        post_rec["date"] =    row.get("CreationDate")

        if row.get("PostTypeId") == "1": 
            # If post is a question, then ThreadID = PostID
            post_rec["threadID"] = int(post_rec["ID"])
        elif row.get("PostTypeId") == "2":
            # Post is an answer
            post_rec["threadID"] = int(row.get("ParentId"))
        else:
            post_rec["threadID"] = 0

        if None in post_rec.values():
            print "skipping post", post_rec["ID"] 
            continue
        
        yield post_rec

def get_threads(file = POST_XML_file):
    xml = etree.parse(open(file))

    for row in xml.findall("row"):
        # skip if not a question post
        if not row.get("PostTypeId") == "1": continue 

        thread_rec = {}
        thread_rec["ID"] =    int(row.get("Id")) 
        thread_rec["title"] = row.get("Title") if row.get("Title") else ""
        
        if None in thread_rec.values():
            print "skipping thread", thread_rec["ID"] 
            continue
        
        yield thread_rec

#
# Create hash tables
#

def get_thread_table(file_name = POST_XML_file):
    table = {}
    for thread in get_threads(file_name):
        table[thread["ID"]] = thread
    return table

def get_post_table(file_name = POST_XML_file):
    table = {}
    for post in get_posts(file_name):
        table[post["ID"]] = post
    return table

def get_user_table(file_name = USER_XML_file):
    table = {}
    for user in get_users(file_name):
        table[user["ID"]] = user
    return table

#
# Combine threads
#
from collections import defaultdict
def get_tp_table(file_name = POST_XML_file):
    # creat thread-post-table tp:
    # tp["threadID"] = [ post1, post2, ... ]
    table = defaultdict(list)
    for post in get_posts(file_name):
        table[post["threadID"]].append(post)

    return table

tp_table = None
def get_complete_threads(file_name = POST_XML_file):
    global tp_table
    if tp_table == None:
        print "Caching thread/posts"
        tp_table = get_tp_table(file_name)
    
    for thread in get_threads(file_name):
        thread["posts"] = tp_table[thread["ID"]]
        yield thread

#
# Tests
#

def TEST():
    print "=== Printing Users ===" 
    for user_rec in get_users():
        PRINT( user_rec )
        
        sign = raw_input()
        if sign == "x": break

    print "=== Printing Threads ==="
    for thread_rec in get_threads():
        PRINT( thread_rec )
        
        sign = raw_input()
        if sign == "x": break
 
    print "=== Printing Posts ==="
    for post_rec in get_posts():
        PRINT( post_rec )
        
        sign = raw_input()
        if sign == "x": break
        
if __name__ == "__main__": TEST()
        