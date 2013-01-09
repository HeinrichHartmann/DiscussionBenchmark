import MySQLdb

db = MySQLdb.connect( host = "localhost",
                      user = "discuss",
                      passwd = "discuss",
                      db   = "discuss" )

cursor = db.cursor();

# drop tables
try: 
    cursor.execute("""DROP TABLE user;""")
    cursor.execute("""DROP TABLE threads;""")
    cursor.execute("""DROP TABLE posts;""")
except:
    print "error dropping tables"


# create Tables
try:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS `user` (
        `ID` int(11),
        `userName` text,
        PRIMARY KEY (`ID`)
    );""")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS `threads` (
          `ID` int(11),
          `threadTitle` text,
          PRIMARY KEY (`ID`)
        );""")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS `posts` (
          `ID` int(11),
          `threadID` int(11),
          `userID` int(11),
          `title` text,
          `content` text,
          `date` date,
          PRIMARY KEY (`ID`)
        );""")
except:
    raise Exception("error creating tables")
    

    