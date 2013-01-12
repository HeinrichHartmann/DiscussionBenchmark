import MySQLdb as db

from ReadXML import XMLReader
from WriteCSV import CSVWriter

USER_TABLE = "users"
THREAD_TABLE = "threads"
POST_TABLE = "posts"

def main():    
    DBO = MySQLControls()
    
    DBO.TEST()
    
    print "Shutting down db"
    DBO.close()

class MySQLControls:
    info = "MySQL"
    
    def __init__(self):
        self.con = db.connect( host = "localhost",
                          user = "discuss",
                          passwd = "discuss",
                          db   = "discuss", 
                        local_infile = 1)    
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
                `userName` text
            );""" % USER_TABLE)

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS `%s` (
                  `ID` int(11),
                  `title` text
            );""" % THREAD_TABLE)

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS `%s` (
                  `ID` int(11),
                  `threadID` int(11),
                  `userID` int(11),
                  `content` text,
                  `date` datetime
                );""" % POST_TABLE)

        except db.Error, e:
            print "error creating tables", e
            raise Exception("error creating tables")

    def reset(self):
        self.reset_cursor()
        self.drop_tables()
        self.create_tables()
    
    def reset_cursor(self):
        self.cursor.close()
        self.cursor = self.con.cursor()        

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
        
    def insert_post(self, ID, threadID, userID, content, date):
        if ID == None: 
            ID = 0

        self.cursor.execute("""
            INSERT INTO {0}(ID, threadID, userID, content, date) VALUES({1:d},{2:d},{3:d},'{4}','{5}');
        """.format(POST_TABLE,
                   ID,
                   threadID, 
                   userID, 
                   esc(content), 
                   date)
            )
        
        
    def insert_users_csv(self, csv_file = "users.csv"):
        self.cursor.execute("""
        LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1} 
            fields terminated by ','  
            enclosed by '' 
            lines terminated by '\n'  
            (ID, userName);
        """.format(csv_file, USER_TABLE))
        self.commit()

    def insert_threads_csv(self, csv_file = "threads.csv"):
        self.cursor.execute("""
        LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1} 
            fields terminated by ','  
            enclosed by '' 
            lines terminated by '\n'  
            (ID, title);
        """.format(csv_file, THREAD_TABLE))
        self.commit()
        
    def insert_posts_csv(self, csv_file = "posts.csv"):
        self.cursor.execute("""
        LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1} 
            fields terminated by ','  
            enclosed by '' 
            lines terminated by '\n'  
            (ID, threadID, userID, content, date);
        """.format(csv_file, POST_TABLE))
        self.commit()
    
    
    def create_indices(self):
        self.cursor.execute("""
        CREATE INDEX thread_id ON {0}(ID);
        
        CREATE INDEX post_id ON {1}(ID);
        CREATE INDEX post_user ON {1}(userID);
        CREATE INDEX post_thread ON {1}(threadID,date DESC);
        
        CREATE INDEX user_id ON {2}(ID);
        """.format(THREAD_TABLE, POST_TABLE, USER_TABLE))
    
    def get_thread(self, threadID):
        self.cursor.execute("""
        SELECT
            `{tt}`.title, 
            `{pt}`.content, 
            `{pt}`.date,
            `{ut}`.userName
        FROM `{tt}`,`{pt}`,`{ut}` 
        WHERE (
             `{pt}`.`threadID` = `{tt}`.`ID` AND
             `{pt}`.`userID` = `{ut}`.`ID`    AND             
             `{tt}`.`ID` = {0}
        );
        """.format(threadID, tt=THREAD_TABLE,pt=POST_TABLE,ut=USER_TABLE))
        return self.cursor.fetchall()
    
    def TEST(self):
        
        print "Insert Test Data"
        self.insert_user(ID = -1, name = "test user")
        self.insert_thread(ID = -1, title = "test thread")
        self.insert_post(ID = -1, threadID = -1, userID = -1, content = "test content", date = "2013-01-01")
        self.con.commit()
        
        for i,row in enumerate(self.get_thread(-1)):
            print i,":", row

    def import_XML(self,XRO):
        name = XRO.name
        self.XRO = XRO
        
        CWO = CSVWriter(XRO)
        print "* Users"
        CWO.write_users(name + "users.csv")        
        self.insert_users_csv(name + "users.csv")
        
        print "* Threads"
        CWO.write_threads(name + "threads.csv")
        self.insert_threads_csv(name + "threads.csv")
        
        print "* Posts"
        CWO.write_posts(name + "posts.csv")
        self.insert_posts_csv(name + "posts.csv")

        print "Writing Indices"
        self.create_indices()
        
        self.reset_cursor()
        
def esc(s):
    return db.escape_string(s.encode('ascii','ignore'))

if __name__ == '__main__':
    main()
