import pymongo

from TimeDec import TimeDec
from ReadXML import XMLReader

DEBUG = False

def main():
    DBO = MongoControls()
    
    DBO.TEST()

class MongoControls:
    # We follow the tutorial
    # http://api.mongodb.org/python/2.4.1/tutorial.html
    
    # Remark:
    # documents are members of collections
    # collections are members of databases

    info = "Mongo"

    DB_NAME = "discuss"
    COL_THREAD = "threads"
    COL_USER   = "users"
    
    def __init__(self, host = 'localhost', port = 27017):
        self.con = pymongo.MongoClient(host, port)
        self.db  = self.con[self.DB_NAME]
        self.tc  = self.db[self.COL_THREAD]
        self.uc  = self.db[self.COL_USER]
        self.XRO  = XMLReader()
        
    def close(self):
        pass
    
    def reset(self):
        self.tc.drop()
        self.uc.drop()

    def insert_thread(self, data):
        # Insert data into thread collection
        # returns id
        return self.tc.insert(data)

    def insert_user(self, data):
        # Insert data into user collection
        # returns id
        return self.uc.insert(data)

    def get_thread(self, ID):
        thread_rec = self.tc.find_one({"ID": ID})

        user_list  = []
        try:
            for post in thread_rec["posts"]:
                user_list.append(self.uc.find({"ID":post["userID"]}))
        except KeyError:
            pass    
        
        return [thread_rec, user_list]
    
    def get_user(self, ID):
        return self.uc.find_one({"ID": ID})
    
    
    def fill_users(self, batch_size = 5000):
        buffer = []
        for user in self.XRO.get_users():
            buffer.append(user)
            
            if len(buffer) == batch_size:
                if DEBUG: print "Writing user collection", batch_size
                self.insert_user(buffer)
                buffer = []

        self.insert_user(buffer)

    def fill_threads(self, batch_size = 5000):
        buffer = []
        for thread in self.XRO.get_complete_threads():
            buffer.append(thread)
            
            if len(buffer) == batch_size:
                if DEBUG: print "Writing thread collection", batch_size
                self.insert_thread(buffer)
                buffer = []

        self.insert_thread(buffer)
    
    def create_indices(self):
        self.uc.create_index("ID")
        self.tc.create_index("ID")
        
    @TimeDec
    def import_XML(self, XRO):
        self.XRO = XRO
        self.fill_threads()
        self.fill_users()

    
    def TEST(self):
        global DEBUG
        DEBUG = 1
        
#        print "Creating test documents"
#        tid = self.insert_thread({"thread":"test", "ID": -1})
#        uid = self.insert_user({"user":"test", "ID": -1})
#        
#        print self.get_thread(-1)
#        print self.get_user(-2)
        
        self.reset()

        print "Populating MongoDB"
        self.fill_users(10000)
        self.fill_threads()

        print self.get_thread(3)
        print self.get_user(3)

        
        print "Creating Indices"
        self.create_indices()
        
        
if __name__ == "__main__": 
    main()