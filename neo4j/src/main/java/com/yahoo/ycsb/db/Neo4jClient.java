package com.yahoo.ycsb.db;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;

/**
 * YCSB binding for Neo4j.
 */
public class Neo4jClient extends DB {

  private static final String URL_PROPERTY = "neo4j.url";
  private static final String URL_PROPERTY_DEFAULT = "bolt://localhost:7687";

  private static final String USER_PROPERTY = "neo4j.user";
  private static final String USER_PROPERTY_DEFAULT = "neo4j";

  private static final String PASSWORD_PROPERTY = "neo4j.password";
  private static final String PASSWORD_PROPERTY_DEFAULT = "1234";
  
  // START SNIPPET: vars
  private static Session session = null;
  private static Driver driver = null;
  // END SNIPPET: vars
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException {
    // initialize Neo4j driver
    synchronized (Neo4jClient.class) {
      final Properties props = getProperties();
      try {
        FileReader reader = new FileReader("./neo4j/src/main/java/com/yahoo/ycsb/db/db.properties");
        props.load(reader);
      } catch (Exception e) {
        System.out.println("Config file for neo4j not found. \nContinuing with default values...");
      }
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
    if (session != null) {
      System.out.println("Cleaning up ...");
      session.close();
      driver.close();

    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {

      if (driver == null) {
        init();
      }

      Map<String, Object> out = new HashMap<>();
      String jsonObj = "{ naturalId: \"" + key + "\"";
      for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        jsonObj += ", " + entry.getKey() + ": $" + entry.getKey();
        out.put(entry.getKey(), entry.getValue());
      }
      jsonObj += "}";

      String statement = "CREATE (" + key + ":" + table + " " + jsonObj + ")";
      TransactionConfig config = TransactionConfig.builder().build();
      session.run(statement, out, config);

      return Status.OK;
    } catch (Exception e) {
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
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (driver == null) {
        init();
      }
      Map<String, Object> out = new HashMap<>();
      String jsonObj = "{ lastUpdatedAt : \"" + java.time.LocalDateTime.now() + "\"";
      for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        jsonObj += ", " + entry.getKey() + ": $" + entry.getKey();
        out.put(entry.getKey(), entry.getKey());
      }
      jsonObj += "}";

      String naturalId = "\"" + key + "\"";
      String statement = "MATCH (n { naturalId: " + naturalId + "}) SET n +=" + jsonObj;
      TransactionConfig config = TransactionConfig.builder().build();
      session.run(statement, out, config);
      System.out.print(key + ": ");
      System.out.println(out);
    } catch (Exception e) {
      System.out.println("Something went wrong 2" + e.getMessage());
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }
}
