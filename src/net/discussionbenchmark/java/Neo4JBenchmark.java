package net.discussionbenchmark.java;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;

import au.com.bytecode.opencsv.CSVReader;

public class Neo4JBenchmark {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		final String PATH = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/NativeNeo4J";
		final String THREAD_FILE = "/home/heinrich/Desktop/eclipse_related-work/DiscussionBenchmark/threads.csv";
		// final Integer RUNS = 1000;
		
	    EmbeddedGraphDatabase DB = new EmbeddedGraphDatabase(PATH);
	    
	    FileReader reader = new FileReader(THREAD_FILE);
	    CSVReader R = new CSVReader(reader,',');
	    String [] line = R.readNext();
	    ArrayList<Integer> threadList = new ArrayList<Integer>();
	    
	    for (String thread_string: line) {
	    	threadList.add(Integer.parseInt((thread_string)));
	    }
	    
	    Index<Node> index = DB.index().forNodes("threadID");
	    
	    
	    long startTime = System.currentTimeMillis();
	    
	    for (int threadID : threadList){
//	    	System.out.println("Retrieving thread" + threadID);
	    	Node t = index.get("ID", threadID).getSingle();

	    	Traverser Q = Traversal.description()
    		.relationships(RelTypes.CONTAINS, Direction.OUTGOING)
    		.relationships(RelTypes.WRITTEN_BY, Direction.OUTGOING)
    		.evaluator(Evaluators.toDepth(2))
    		.traverse(t);
    		
	    	for( Path threadPath : Q ) {
				threadPath.endNode().getPropertyValues().toString();
//	    		System.out.println(
//	    				threadPath.endNode().getPropertyValues().toString()
//	    				);
	    	}
	    }
	    
	    long endTime = System.currentTimeMillis();
	   	
	    System.out.println("Retrieved " + threadList.size() + " threads");
	    System.out.println("Total time: " + (endTime - startTime));

	    DB.shutdown();
	}

    public enum RelTypes implements RelationshipType
    {
        CONTAINS,
        WRITTEN_BY,
    }

	
}
