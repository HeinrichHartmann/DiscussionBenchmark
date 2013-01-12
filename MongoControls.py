import pymongo

from ReadXML import get_users, get_complete_threads

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
        return self.tc.find_one({"_id": ID})
    
    def get_user(self, ID):
        return self.uc.find_one({"_id": ID})
    
    
    def fill_users(self, batch_size = 5000):
        buffer = []
        for user in get_users():
            buffer.append(user)
            
            if len(buffer) == batch_size:
                print "Writing user collection", batch_size
                self.insert_user(buffer)
                buffer = []

        self.insert_user(buffer)

    def fill_threads(self, batch_size = 5000):
        buffer = []
        for thread in get_complete_threads():
            buffer.append(thread)
            
            if len(buffer) == batch_size:
                print "Writing thread collection", batch_size
                self.insert_thread(buffer)
                buffer = []

        self.insert_thread(buffer)
    
    def create_indices(self):
        self.uc.create_index("ID")
        self.tc.create_index("ID")
    
    
    def TEST(self):
        print "Creating test documents"
        tid = self.insert_thread({"thread":"test"})
        uid = self.insert_user({"user":"test"})
        
        print self.get_thread(tid)
        print self.get_user(uid)
        
        self.reset()

        print "Populating MongoDB"
        self.fill_users(10000)
        self.fill_threads()
        
        print "Creating Indices"
        self.create_indices()
        
        
if __name__ == "__main__": 
    main()