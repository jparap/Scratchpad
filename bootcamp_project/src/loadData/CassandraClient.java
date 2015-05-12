package loadData;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.querybuilder.*;

public class CassandraClient {

	private Cluster cluster;
	private Session session;
		
	public void connect(String node) {
			cluster = Cluster.builder()
			         .addContactPoint(node)
			         .build();
			session = cluster.connect();
		}
		
	public void close() {	
		session.close();
		cluster.close();
			}
	
	public void insert(String table, List<String>keys, List<Object>values){
		Insert insert;
		
//		List<String> keysList = new ArrayList(keyvalues.keySet());
//		List<String> valuesList = new ArrayList(keyvalues.values());
	
		String loadkeys[];
		Object loadvalues[];
		loadkeys = keys.toArray(new String[0]);
		loadvalues = values.toArray();
		
		insert = QueryBuilder.insertInto(table).values(loadkeys, loadvalues);
		try {
			session.execute(insert);
			LoadData.tb1count ++; // inc table counters if no error
		}
		catch (Exception e) {
	         e.printStackTrace();
	         System.out.println(insert.toString());
	         LoadData.tb1err ++;
	      } 
	}

	public void runQuery(String query){
		SimpleStatement statement = new SimpleStatement(query);
		try {
			ResultSet results = session.execute(statement);
			for (Row row: results) {
				System.out.println(row.toString());
			}
		}
		catch (NoHostAvailableException e) {
	         e.printStackTrace();
	      } 
		catch (QueryExecutionException e) {
	         e.printStackTrace();
	      } 
		catch (QueryValidationException e) {
	         e.printStackTrace();
	      } 
		catch (IllegalStateException e) {
	         e.printStackTrace();
	      }
	}
	public void quickTest(){
		   Metadata metadata = cluster.getMetadata();
		   System.out.printf("Connected to cluster: %s\n", 
		         metadata.getClusterName());
		   for ( Host host : metadata.getAllHosts() ) {
		      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
		         host.getDatacenter(), host.getAddress(), host.getRack());
		   }
		
	
	}
	
}
