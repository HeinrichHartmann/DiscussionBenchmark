import MySQLdb as db

USER_TABLE = "users"
THREAD_TABLE = "threads"
POST_TABLE = "posts"

def main():    
    DBO = MySQLControls()
    
    DBO.TEST()
    
    print "Shutting down db"
    DBO.close()

class MySQLControls:
    
    def __init__(self):
        self.con = db.connect( host = "localhost",
                          user = "discuss",
                          passwd = "discuss",
                          db   = "discuss" )    
        self.cursor = self.con.cursor();

    def close(self):
        self.cursor.close();
        self.con.close();

    def commit(self):
        self.con.commit()
        
    def drop_tables(self):
        try: 
            self.cursor.execute("""DROP TABLE IF EXISTS %s;""" % USER_TABLE )
            self.cursor.execute("""DROP TABLE IF EXISTS %s;""" % THREAD_TABLE)
            self.cursor.execute("""DROP TABLE IF EXISTS %s;""" % POST_TABLE)
        except db.Error, e:
            print "error dropping tables", e
        
    def create_tables(self):
        # create Tables
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS `%s` (
                `ID` int(11),
                `userName` text,
                PRIMARY KEY (`ID`)
            );""" % USER_TABLE)

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS `%s` (
                  `ID` int(11),
                  `title` text,
                  PRIMARY KEY (`ID`)
                );""" % THREAD_TABLE)

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS `%s` (
                  `ID` int(11),
                  `threadID` int(11),
                  `userID` int(11),
                  `title` text,
                  `content` text,
                  `date` datetime,
                  PRIMARY KEY (`ID`)
                );""" % POST_TABLE)

        except db.Error, e:
            print "error creating tables", e
            raise Exception("error creating tables")

    def reset_tables(self):
        print "Deleting Tables"
        self.drop_tables()
        self.con.commit()
        
        print "Creating Tables"
        self.create_tables()
        self.con.commit()
            
    def insert_user(self, ID, name):
        if ID == None: 
            ID = 0

        self.cursor.execute("""
        INSERT INTO {0}(ID, userName) VALUES({1:d},'{2}');
        """.format(USER_TABLE,
                   ID,
                   esc(name)
                   )
        )
    
    def insert_thread(self, ID, title):
        if ID == None:
            ID = 0
        
        self.cursor.execute("""
            INSERT INTO {0}(ID, title) VALUES({1:d},'{2}');
        """.format(THREAD_TABLE,
                   ID,
                   esc(title)
                   )
        )
        
    def insert_post(self, ID, threadID, userID, title, content, date):
        if ID == None: 
            ID = 0

        self.cursor.execute("""
            INSERT INTO {0}(ID, threadID, userID, title, content, date) VALUES({1:d},{2:d},{3:d},'{4}','{5}','{6}');
        """.format(POST_TABLE,
                   ID,
                   threadID, 
                   userID, 
                   esc(title), 
                   esc(content), 
                   date)
            )
        
    def TEST(self):
        print "Insert Test Data"
        self.reset_tables()
        
        self.insert_user(ID = 1, name = "test user")
        self.insert_thread(ID = 0, title = "test thread")
        self.insert_post(ID = 0, threadID = 0, userID = 0, title = "test post", content = "test content", date = "2013-01-01")
        self.con.commit()
        
def esc(s):
    return db.escape_string(s.encode('ascii','ignore'))

if __name__ == '__main__':
    main()