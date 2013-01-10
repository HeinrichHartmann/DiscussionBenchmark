import csv

from ReadXML import get_threads, get_posts, get_users

def main():
    write_users()
    write_threads()
    write_posts()

def write_users(file_name = "users.csv"):
    with open(file_name, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(
            ([user["ID"], esc(user["name"])] for user in get_users())              
            )
        
def write_threads(file_name = "threads.csv"):
    with open(file_name, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows( 
            (
             [ thread["ID"], esc(thread["title"]) ]
             for thread in get_threads())
             )


def write_posts(file_name = "posts.csv"):
    with open(file_name, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows((
             [post["ID"],
             post["threadID"], 
             post["userID"],
             esc(post["content"]),
             post["date"].replace("T"," ")[0:19]
             ]
             for post in get_posts())
             )

import re
def esc(s = ""):
#    s = re.sub("""[\n,\t"']""",'',s)
    s = re.sub("""[^A-Za-z0-9\ ]""",'',s)
    return s.encode('ascii','ignore')

if __name__ == "__main__": main()