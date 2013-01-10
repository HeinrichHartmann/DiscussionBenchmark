from MySQLControls import MySQLControls
from ReadXML import get_threads, get_posts, get_users


DBO = MySQLControls()

def main():
    DBO.reset_tables();
    insert_users()
    
def insert_users(max = 0):
    i = 0
    for user in get_users():
        DBO.insert_user(ID=user["ID"], 
                        name = user["name"])
        
        if i == max: break
        
        i += 1
        if i % 1000 == 0:
            print "adding user", i
            DBO.commit()

    DBO.commit()
    
def insert_threads():
    i = 0
    for thread in get_threads():
        DBO.insert_thread(ID=thread["ID"], 
                          title = thread["title"])
        
        i += 1
        if i % 1000 == 0:
            print "adding thread", i
            DBO.commit()
            
    if i == max: break

    DBO.commit()

def insert_posts():
    i = 0
    for post in get_posts():       
        DBO.insert_post(ID = post["ID"], 
                        threadID = post["threadID"], 
                        userID = post["userID"],
                        title = post["title"],
                        content = post["content"],
                        date = post["date"]
                        )

        i += 1
        if i % 1000 == 0:
            print "adding post", i
            DBO.commit()

    DBO.commit()

if __name__ == "__main__": main()