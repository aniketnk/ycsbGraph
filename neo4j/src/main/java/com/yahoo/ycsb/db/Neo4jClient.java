package com.yahoo.ycsb.db;
import java.util.* ;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import java.io.File;
import java.io.*; 
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.driver.* ;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Status;

public class Neo4jClient extends DB {
	private static final String URL_PROPERTY         = "neo4j.url";
  	private static String URL_PROPERTY_DEFAULT = "";

  	private static final String USER_PROPERTY         = "neo4j.user";
  	private static String USER_PROPERTY_DEFAULT = "";

  	private static final String PASSWORD_PROPERTY         = "neo4j.password";
  	private static String PASSWORD_PROPERTY_DEFAULT = "";
	/**
  * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
  */

	// START SNIPPET: vars
	private static GraphDatabaseService graphDb = null;
	Node firstNode;
	Node secondNode;
	Relationship relationship;
	Session session = null;
	Driver driver = null;
	// END SNIPPET: vars
	// START SNIPPET: createReltype
	private static enum RelTypes implements RelationshipType {
		KNOWS
	}

	public void init() throws DBException {
		// initialize Neo4j driver
		synchronized(Neo4jClient.class) {
			try{
				FileReader reader=new FileReader("./neo4j/src/main/java/com/yahoo/ycsb/db/db.properties");  
				Properties externProps = new Properties(); 
				externProps.load(reader);
				URL_PROPERTY_DEFAULT = externProps.getProperty("URL_PROPERTY_DEFAULT");
				USER_PROPERTY_DEFAULT = externProps.getProperty("USER_PROPERTY_DEFAULT");
				PASSWORD_PROPERTY_DEFAULT = externProps.getProperty("PASSWORD_PROPERTY_DEFAULT");
				
			}
			catch(Exception e){
				e.printStackTrace();
			}

			final Properties props = getProperties();
			String url = props.getProperty(URL_PROPERTY, URL_PROPERTY_DEFAULT);
			String user = props.getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);
			String password = props.getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);
			if (driver == null) {
				System.out.println("Making connection");

				driver = GraphDatabase.driver(url, AuthTokens.basic(user, password));
				System.out.println("Starting session");
				session = driver.session();
				session.run("CREATE INDEX ON :usertable(naturalId)");

				System.out.println("Driver is set up and session is started");
			}
		}
	}

	@Override
	public void cleanup() throws DBException {
		if (graphDb != null) {
			System.out.println("Cleaning up ...");
			session.close();
			driver.close();

		}
	}
	private String byteIteratorToString(ByteIterator byteIter) {
		return new String(byteIter.toArray());
	}

	private ByteIterator stringToByteIterator(String content) {
		return new StringByteIterator(content);
	}

	@Override
	public Status insert(String table, String key, Map < String, ByteIterator > values) {
		try {

			if (driver == null) init();

			Map < String, Object > out = new HashMap < >();
			String jsonObj = "{ naturalId: \""+ key+"\"";
			for (Map.Entry < String, String > entry: StringByteIterator.getStringMap(values).entrySet()) {
				jsonObj += ", " + entry.getKey() + ": $" + entry.getKey();
				out.put(entry.getKey(), entry.getValue());
			}
			jsonObj += "}";

			String statement = "CREATE (" + key + ":" + table + " " + jsonObj + ")";
			TransactionConfig config = TransactionConfig.builder().build();
			session.run(statement, out, config);
				
			return Status.OK;
		} catch(Exception e) {
			System.out.println("Something went wrong 1" + e.getMessage());
			e.printStackTrace();
		}
		return Status.ERROR;
	}	

	@Override
	public Status delete(String table, String key) {
		return Status.OK;
	}

	@Override
	public Status read(String table, String key, Set < String > fields, Map < String, ByteIterator > result) {
		return Status.OK;
	}

	@Override
	public Status update(String table, String key, Map < String, ByteIterator > values) {


		// MATCH (n { name: 'Andy' })
		// SET n.surname = 'Taylor'
		// RETURN n.name, n.surname
		try{
			if(driver == null){
					init();
			}
			Map < String, Object > out = new HashMap < >();
			String jsonObj = "{ lastUpdatedAt : \"" + java.time.LocalDateTime.now() +"\"";
			for (Map.Entry < String, String > entry: StringByteIterator.getStringMap(values).entrySet()) {
				jsonObj += ", " + entry.getKey() + ": $" + entry.getKey();
				out.put(entry.getKey(), "Find me");
			}
			jsonObj += "}";

			String naturalId = "\"" + key + "\"";
			String statement = "MATCH (n { naturalId: " + naturalId + "}) SET n +=" + jsonObj;
			TransactionConfig config = TransactionConfig.builder().build();
			session.run(statement,out,config);
			System.out.print(out);
		}
		catch(Exception e){
			System.out.println("Something went wrong 2" + e.getMessage());
			e.printStackTrace();
		}


		return Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set < String > fields, Vector < HashMap < String, ByteIterator >> result) {
		return Status.OK;
	}
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {@Override
			public void run() {
				try {
					graphDb.shutdown();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
}