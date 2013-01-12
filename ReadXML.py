from lxml import etree
from pprint import pprint as PRINT
from collections import defaultdict

# Super User Dataset
USER_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/users.xml"
POST_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/posts.xml"
# Latex Dataset (8959 Threads/ 6375 Users) 
USER_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/users.xml"
POST_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/posts.xml"

def main():
    reader = XMLReader()
    reader.TEST()


class XMLReader:
    def __init__(self, 
                USER_XML = USER_XML_default,
                POST_XML = POST_XML_default
                ):
        self.USER_XML = USER_XML
        self.POST_XML = POST_XML 
    
    user_count = None
    post_count = None
    thread_count = None
    
    #
    # Create Generators
    #
    def get_users(self):
        # yields user recs
        # side: sets user_count
        fh = open(self.USER_XML)
        xml = etree.parse(fh)
        
        yield {"ID":-1, "name": "Annonymous"}

        count = 0
        
        for row in xml.findall("row"):
            user_rec = {}
    
            user_rec["ID"] = int(row.get("Id"))
            user_rec["name"] = row.get("DisplayName")
            if None in user_rec.values(): 
                print "skipping user", user_rec["ID"]
                continue
    
            yield user_rec
            count += 1
            
        fh.close()
        self.user_count = count 
    
    def get_posts(self):
        fh = open(self.POST_XML)
        xml = etree.parse(fh)
        
        count = 0
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
            count +=1
        self.post_count = count
        fh.close()
    
    def get_threads(self):
        fh = open(self.POST_XML)
        xml = etree.parse(fh)
    
        count = 0
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
            count += 1
        fh.close()
        self.thread_count = count
        
    #
    # Create hash tables
    #
    thread_table = None
    def get_thread_table(self):
        if self.thread_table: return self.thread_table
        table = {}
        for thread in self.get_threads():
            table[thread["ID"]] = thread
            
        self.thread_table = table
        return table

    post_table = None   
    def get_post_table(self):
        if self.post_table: return self.post_table
        table = {}
        for post in self.get_posts():
            table[post["ID"]] = post
            
        self.post_table = table
        return table
    
    user_table = None
    def get_user_table(self):
        if self.user_table: return self.user_table
        table = {}
        for user in self.get_users():
            table[user["ID"]] = user
            
        self.user_table = table
        return table

    tp_table = None
    def get_tp_table(self):
        # creat thread-post-table tp:
        # tp["threadID"] = [ post1, post2, ... ]
        if self.tp_table: return self.tp_table
        
        table = defaultdict(list)
        for post in self.get_posts():
            table[post["threadID"]].append(post)

        self.tp_table = table    
        return table
    
    #
    # Combine threads
    #
        
    def get_complete_threads(self):
        tp_table = self.get_tp_table()
        
        for thread in self.get_threads():
            thread["posts"] = tp_table[thread["ID"]]

            yield thread

    #
    # Statistics
    # 
    def get_post_count(self):
        if self.post_count: return self.post_count
        for rec in self.get_posts():
            pass
        return self.post_count
    def get_thread_count(self):
        if self.thread_count: return self.thread_count
        for rec in self.get_threads():
            pass
        return self.thread_count
        
    def get_user_count(self):
        if self.user_count: return self.user_count
        for rec in self.get_users():
            pass
        return self.user_count
    
    def get_stats(self):
        return {
            "POST_XML": self.POST_XML,
            "USER_XML": self.USER_XML,
            "thread_count": self.get_thread_count(),
            "post_count":   self.get_post_count(),
            "user_count":   self.get_user_count()
            }
    #
    # Tests
    #
    
    def TEST(self):
        print "=== Printing Users ===" 
        for user_rec in self.get_users():
            PRINT( user_rec )
            
            sign = raw_input()
            if sign == "x": break
    
        print "=== Printing Threads ==="
        for thread_rec in self.get_threads():
            PRINT( thread_rec )
            
            sign = raw_input()
            if sign == "x": break
     
        print "=== Printing Posts ==="
        for post_rec in self.get_posts():
            PRINT( post_rec )
            
            sign = raw_input()
            if sign == "x": break
        
        print "=== STATS ==="
        PRINT( self.get_stats() )
        
if __name__ == "__main__": 
    main()
        