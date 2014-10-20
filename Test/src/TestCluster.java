import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.datastax.driver.core.Cluster.*;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.policies.LatencyAwarePolicy.Builder;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.*;

/**
 * Test class primarily for testing t12775
 * 
 * @author mark
 * 
 *
 */
public class TestCluster {

	public static void main(String args[]) {
		TestCluster myTest = new TestCluster();

		// CassandraClient myWorkingTest = new CassandraClient();
		// myWorkingTest.connect("192.168.56.101");
		// myWorkingTest.
		// myWorkingTest.runQuery("describe keyspaces;");
		// myWorkingTest.close();

		Cluster myCluster = null;
		Session mySession = null;
		ResultSet myResult;
		String myStatements[] = new String[2];
		// myCluster = myTest.createCluster("mark", "password", "DC1",
		// Boolean.FALSE, "192.168.56.20");

		try {
			myCluster = myTest
					.createCluster("/Users/mark/Issues/Restlet_27409329/12775/test.properties");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			mySession = myCluster.connect();
			myStatements[0] = "use system;";
			myStatements[1] = "select * from peers;";
			Collection myHosts = mySession.getState().getConnectedHosts();

			for (Object myHost : myHosts) {
				System.out.println("Connected hosts: " + myHost.toString());
			}

			for (String myStatement : myStatements) {
				myResult = mySession.execute(myStatement);
				for (Row row : myResult) {
					System.out.println(row.toString());
				}
			}
		} catch (NoHostAvailableException e) {
			System.out.println(e.getMessage());
			System.out.println(e.getErrors().toString());
		}
		mySession.close();
		System.exit(0);
	}

	public static Map<String, String> readProperties(String filePath)
			throws IOException {
		Properties props = new Properties();
		FileInputStream in = new FileInputStream(filePath);
		props.load(in);
		in.close();
		Map<String, String> map = new HashMap<String, String>();
		for (Entry<Object, Object> objProp : props.entrySet()) {
			map.put(objProp.getKey().toString(), objProp.getValue().toString());
		}
		return map;
	}

	public Cluster createCluster(String properties) throws Exception {
		// Initializing pool
		List<String> contactPoints = new ArrayList<String>();
		Map<String, String> map = new HashMap<String, String>();
		map = readProperties(properties);
		// PropertiesUtils.loadProperties(propertiesPath, map);
		int index = 0;
		String line = null;
		while ((line = map.get("cassandra.contact.point." + index)) != null) {
			System.out.println("Contact point : " + line);
			contactPoints.add(line);
			index++;
		}
		String localDcRegion = map.get("dc.local.region");
		String whiteList = map.get("cassandra.white.list");
		String login = map.get("cassandra.login");
		String password = map.get("cassandra.password");
		Cluster cluster = null;
		try {
			if (contactPoints.isEmpty()) {
				System.out.println("Local cluster");
				// cluster = DatastaxDriverUtils.createCluster(login,
				// password);// note overloaded method names
				// in the same class, we dont have this one from the customer,
				// but in our testing, we dont really need it
			} else {
				System.out.println("Cluster with contact points");
				// Use the defined contact points
				String[] tab = new String[contactPoints.size()];
				cluster = createCluster(login, password, localDcRegion,
						Boolean.parseBoolean(whiteList),
						contactPoints.toArray(tab));
			}
		} catch (NoHostAvailableException ex) {
			System.out.println("The cassandra database can't be reached.");
		}

		if (cluster == null) {
			System.out
					.println("The cluster is not initialized. Check your configuration, or the database");
			System.out.println("Cluster properties:");
			if (!contactPoints.isEmpty()) {
				for (int i = 0; i < contactPoints.size(); i++) {
					System.out.println(" - contact point " + i + ": " + line);
				}
			} else {
				System.out.println(" - no contact point configured.");
			}
			if (localDcRegion != null) {
				System.out.println(" - EC2 local region: " + localDcRegion);
			} else {
				System.out.println(" - no EC2 local region configured.");
			}
			if (login != null) {
				System.out.println(" - login provided: " + login);
			} else {
				System.out.println(" - no login provided.");
			}
			if (password != null) {
				System.out.println(" - password provided");
			} else {
				System.out.println(" - no password provided.");
			}
			throw new Exception("The cassandra database can't be reached.");
		}
		return cluster;

	}

	public static Cluster createCluster(String login, String password,
			String localDcRegion, boolean isWhiteList, String... contactsPoints) {

		List<InetSocketAddress> cpList = new ArrayList<InetSocketAddress>();
		for (String contactPoint : contactsPoints) {
			InetSocketAddress isa = null;
			int index = contactPoint.indexOf(":");
			if (index == -1) {
				isa = new InetSocketAddress(contactPoint,
						ProtocolOptions.DEFAULT_PORT);
			} else {
				isa = new InetSocketAddress(contactPoint.substring(0, index),
						Integer.parseInt(contactPoint.substring(index + 1)));
			}
			cpList.add(isa);
		}
		com.datastax.driver.core.Cluster.Builder builder = Cluster.builder();
		builder.addContactPointsWithPorts(cpList);

		LoadBalancingPolicy lbp = null;
		// if (localDcRegion != null) {
		if (!localDcRegion.isEmpty()) {
			lbp = new DCAwareRoundRobinPolicy(localDcRegion);
		} else {
			lbp = new RoundRobinPolicy();
		}
		System.out.println(lbp.toString());
		System.out.println("localDcRegion: " + localDcRegion);
		System.out.println("localDcRegion isEmpty(): "
				+ localDcRegion.isEmpty());

		if (isWhiteList) {
			if (cpList.isEmpty()) {
				try {
					throw new Exception(
							"Please provide a list of valid contact points to the database.");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			builder.withLoadBalancingPolicy(new WhiteListPolicy(lbp, cpList));
		} else {
			builder.withLoadBalancingPolicy(lbp);
		}

		if (login != null) {
			builder.withAuthProvider(new PlainTextAuthProvider(login, password));
		}

		Cluster cluster = builder.build();

		PoolingOptions pools = cluster.getConfiguration().getPoolingOptions();

		pools.setCoreConnectionsPerHost(HostDistance.REMOTE, 2);
		pools.setMaxConnectionsPerHost(HostDistance.REMOTE, 2);

		return cluster;
	}

}
