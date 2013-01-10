from lxml import etree
from pprint import pprint as PRINT

USER_XML_file = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/users.xml"
POST_XML_file = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/posts.xml"

def get_users(file = USER_XML_file):
    xml = etree.parse(open(file))
    
    for row in xml.findall("row"):
        user_rec = {}

        user_rec["ID"] = int(row.get("Id"))
        user_rec["userName"] = row.get("DisplayName")
        
        yield user_rec


def get_posts(file = POST_XML_file):
    xml = etree.parse(open(file))
    
    for row in xml.findall("row"):
        post_rec = {}

        post_rec["ID"] =      int(row.get("Id")) 
        post_rec["userID"] =  row.get("OwnerUserId")
        post_rec["title"] =   row.get("Title")
        post_rec["content"] = row.get("Body")
        post_rec["date"] =    row.get("CreationDate")

        if row.get("PostTypeId") == "1": 
            # If post is a question, then ThreadID = PostID
            post_rec["threadID"] = post_rec["ID"]              
        elif row.get("PostTypeId") == "2":
            # Post is an answer
            post_rec["threadID"] = row.get("ParentId")
            
        yield post_rec

def get_threads(file = POST_XML_file):
    xml = etree.parse(open(file))
    
    for row in xml.findall("row"):
        # skip if not a question post
        if not row.get("PostTypeId") == "1": continue 

        thread_rec = {}
        thread_rec["ID"] =      int(row.get("Id")) 
        thread_rec["title"] =   row.get("Title")
            
        yield thread_rec


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
        