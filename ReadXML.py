from lxml import etree
from pprint import pprint as PRINT
from collections import defaultdict
from random import choice

# Super User Dataset
USER_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/users.xml"
POST_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 Super User/posts.xml"
# Latex Dataset (8959 Threads/ 6375 Users) 
USER_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/users.xml"
POST_XML_default = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/092011 TeX - LaTeX/posts.xml"

DEBUG = True

MAXELEM = 1000000

def main():
    reader = XMLReader()
    reader.TEST()


class XMLReader:
    def __init__(self, 
                USER_XML = USER_XML_default,
                POST_XML = POST_XML_default,
                name = "",
                max  = MAXELEM
                ):
        self.USER_XML = USER_XML
        self.POST_XML = POST_XML 
        self.name = name
        self.max = max
    
    @classmethod
    def from_dataset(cls, DATASET):
        return XMLReader(
           USER_XML = DATASET["USER_XML"],
           POST_XML = DATASET["POST_XML"],
           name =     DATASET["name"],
           max  =     DATASET["max"] if "max" in DATASET else MAXELEM 
        )
    
    
    ### Get/Set Users ###
    user_list = None
    user_table = None
    user_count = None
    def set_users(self):
        xml = etree.iterparse(self.USER_XML, events = ('end',), tag="row")
        
        # Create user for annonymous posts
        self.user_list = [ {"ID":-1, "name": "Annonymous"} ]
        self.user_table = {}

        count = 0
        
        for event, elem in xml:
            user = {}
    
            user["ID"] = int(elem.get("Id"))
            user["name"] = elem.get("DisplayName")

            # Free memory http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
            elem.clear()
            #  Also eliminate now-empty references from the root node to <Title> 
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            
            if None in user.values(): 
                if DEBUG: print "skipping user", user["ID"]
                continue
    
            self.user_list.append(user)
            self.user_table[user["ID"]] = user
            count += 1

            
            if count == self.max: break
        
        xml = None
        self.user_count = count
        
    def get_users(self):
        if not self.user_list: self.set_users()
        return self.user_list

    def get_user_table(self):
        if not self.user_table: self.set_users()
        return self.user_table
    
    def get_user_count(self):
        if not self.user_count: self.set_users()
        return self.user_count
    
    ### Get/Set Post ###
    post_list  = None
    post_table = None
    post_count = None
    tp_table   = None
    def set_posts(self):
        xml = etree.iterparse(self.POST_XML, events = ('end',), tag="row")
        
        self.post_list = []
        self.post_table = {}
        self.tp_table   = defaultdict(list)
        
        count = 0
        for event, elem in xml:
            post = {}
    
            post["ID"] =      int(elem.get("Id"))
            post["userID"] =  int(elem.get("OwnerUserId")) if elem.get("OwnerUserId") else -1
            post["content"] = elem.get("Body")
            post["date"] =    elem.get("CreationDate")
    
            if elem.get("PostTypeId") == "1": 
                # If post is a question, then ThreadID = PostID
                post["threadID"] = int(post["ID"])
            elif elem.get("PostTypeId") == "2":
                # Post is an answer
                post["threadID"] = int(elem.get("ParentId"))
            else:
                post["threadID"] = 0
    
            # Free memory http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
            elem.clear()
            #  Also eliminate now-empty references from the root node to <Title> 
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    
            if None in post.values():
                if DEBUG: print "skipping post", post["ID"] 
                continue
            
            self.post_list.append(post)
            self.post_table[post["ID"]] = post
            self.tp_table[post["threadID"]].append(post)
                        
            count +=1
            if count == self.max: break
            
        self.post_count = count
        
    def get_posts(self):
        if not self.post_list: self.set_posts()
        return self.post_list

    def get_post_table(self):
        if not self.post_table: self.set_posts()
        return self.post_table
    
    def get_tp_table(self):
        if not self.tp_table: self.set_posts()
        return self.tp_table
    
    def get_post_count(self):
        if not self.post_count: self.set_posts()
        return self.post_count


    ### Get/Set Threads ###
    thread_count = None
    thread_table = None
    thread_list = None
    def set_threads(self):
        xml = etree.iterparse(self.POST_XML, events = ('end',), tag="row")
        
        self.thread_list = []
        self.thread_table = {}
        
        count = 0
        for event, elem in xml:
            # skip if not a question post
            if not elem.get("PostTypeId") == "1": continue 
    
            thread = {}
            thread["ID"] =    int(elem.get("Id")) 
            thread["title"] = elem.get("Title") if elem.get("Title") else ""
            
            # Free memory http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
            elem.clear()
            #  Also eliminate now-empty references from the root node to <Title> 
            while elem.getprevious() is not None:
                del elem.getparent()[0]
             
            if None in thread.values():
                if DEBUG: print "skipping thread", thread["ID"] 
                continue
            
            self.thread_list.append(thread)
            self.thread_table[thread["ID"]] = thread

            count += 1
            
            if count == self.max: break

        
        xml = None
        self.thread_count = count
        
    def get_threads(self):
        if not self.thread_list: self.set_threads()
        return self.thread_list

    def get_thread_table(self):
        if not self.thread_table: self.set_threads()
        return self.thread_table
    
    def get_thread_count(self):
        if not self.thread_count: self.set_threads()
        return self.thread_count

    #
    # Derived methods
    #
    def get_complete_threads(self):
        # returns thread with post table
        tp_table = self.get_tp_table()
        
        for thread in self.get_threads():
            thread["posts"] = tp_table[thread["ID"]]
            yield thread


    def get_user_ids(self):
        return self.get_user_table().keys()

    def get_thread_ids(self):
        return self.get_thread_table().keys()

    def get_post_ids(self):
        return self.get_post_table().keys()

    def get_thread_sample(self, count):
        threads = self.get_thread_ids()
        return [ choice(threads) for i in range(count) ]

    def get_post_sample(self, count):
        posts = self.get_post_ids()
        return [ choice(posts) for i in range(count) ]

    def get_user_sample(self, count):
        users = self.get_user_ids()
        return [ choice(users) for i in range(count) ]


    #
    # Statistics
    #
    
    def get_stats(self):
        return {
            "POST_XML": self.POST_XML,
            "USER_XML": self.USER_XML,
            "thread_count": self.get_thread_count(),
            "post_count":   self.get_post_count(),
            "user_count":   self.get_user_count()
            }
        
    def get_thread_length(self, thread_ID):
        return len(self.get_tp_table()[thread_ID])
        
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
            PRINT( "NumPosts:" + str(self.get_thread_length(thread_rec["ID"])) )
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
        