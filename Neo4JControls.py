import requests
import json
from urlparse import urljoin
# See http://stackoverflow.com/questions/10893374/python-confusions-with-urljoin
# for usage pitfalls

class Neo4JControls:
    info = "Neo4J"
    def __init__(self, url = "http://localhost:7474/"):         
        self.url = urljoin(url,"db/data/")

    def post(self, url = None, data = {}):
        if url == None:
            url = self.url

        r = requests.post(url, json.dumps(data))

        if r.ok == True:
            return json.loads(r.content)
        else:
            print "ERROR in POST request to", url, "with", data
            print r
            print dir(r)
            raise Exception("POST Request Error.")

    def get(self, url = None):
        if url == None:
            url = self.url

        r = requests.get(url)
        if r.ok == True:
            return json.loads(r.content)
        else:
            raise Exception("GET Request Error.")

    def create_node(self, data = {}):
        # creates node with properties data
        # returns url of created node

        return self.post(url=urljoin(self.url,"node/"), data=data)["self"] 

    def get_properties(self, url):
        # works for nodes and relationships
        return self.get(url + "/properties")

    def create_relation(self, source, target, relation_type = "", data = {}):
        return self.post(url = source + "/relationships",
                         data={
                               "to":   target,
                               "type": relation_type,
                               "data": data
                               }
                         )["self"] 

    def import_XML(self, XRO):
        self.XRO = XRO
        pass
    
    def reset(self):
        pass
    
    def close(self):
        pass

def TEST():
    DBO = Neo4JControls()
    
    print "Creating Node"    
    node = DBO.create_node({'testKey':'testValue'})
    print "Node", node, "created"
    
    print "List Node Properties"
    print DBO.get_properties(node)
    

    print "Creating test relation"
    node2 = DBO.create_node({'testKey':'target'})
    rel = DBO.create_relation(source = node, target=node2, 
                              relation_type="TESTTYPE", 
                              data={"relKey": "relValue"} )
    print "Relation", rel, "created"
    print DBO.get_properties(rel)

if __name__ == "__main__":
    TEST()
    
    
    
    
    
    
    