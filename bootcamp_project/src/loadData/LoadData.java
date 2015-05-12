package loadData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import loadData.CassandraClient;

/*
 * Loads data for bootcamp excercise
 */


public class LoadData {
	
	private String outputLine;
	private String outputBuilder [];
	CassandraClient client;
	Map <String, String> keyvalues;
	Map <String, String> keyvalues2;
	String csvLine = "";
	String delimiter = "|";
	BufferedWriter writer;
	Map<String, String> valueTypes;
	List<String> setkeys;
	List<Object> values;
	List<String> setkeys2;
	List<Object> values2;
	String node;
	String table;	
	String table2;
	Integer limit;
	public static Integer tb1count;
	public static Integer tb2count;
	public static Integer tb1err;
	public static Integer tb2err;
	
	// Keyspace and table definition for reference
	
	/*
	CREATE KEYSPACE IF NOT EXISTS finefoods WITH replication "
    = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    
	create table reviews_by_day (
			date TEXT,
			time TIMESTAMP,
			prod_id TEXT,
			user_id TEXT,
			profileName TEXT,
			helpfulness TEXT,
			score	DOUBLE,
			summary TEXT,
			text TEXT
			PRIMARY KEY (date, time,prod_id, user_id)
		)
		WITH CLUSTERING ORDER BY (time DESC)
	*/
	
	public LoadData(){
		// Initialise some values
		client = new CassandraClient();
		keyvalues = new HashMap<String, String>();
		keyvalues2 = new HashMap<String, String>();
		valueTypes = new HashMap<String, String>();
		valueTypes.clear(); // clear down previous
		valueTypes.put("date","text"); // date
		valueTypes.put("time","timestamp"); // timestamp
		valueTypes.put("prod_id","text"); // product id
		valueTypes.put("user_id","text"); // user id
		valueTypes.put("profileName","text"); // profile name
		valueTypes.put("helpfulness","text"); // helpfulness
		valueTypes.put("score","double"); // score
		valueTypes.put("summary","text"); // summary
		valueTypes.put("text","text"); // review text	
		node = ""; // node ip
		table = "reviews_by_day"; // table name
		table2 = "reviews_by_hour"; // table name
		setkeys = new ArrayList<>(); // these have to be init here or all sorts of silliness happens
		setkeys2 = new ArrayList<>(); // these have to be init here or all sorts of silliness happens
		values = new ArrayList<>(); // as above
		values2 = new ArrayList<>(); // as above
		limit = 0;
		tb1count = 0;
		tb2count = 0;
		tb1err = 0;
		tb2err = 0;
		
	}


	public static void main (String args []){	
		Path inputFilePath;
		Path outputFilePath;
		Path tableFilePath;
		Path table2FilePath;
		Path keyspaceFilePath;
		// Setup file paths
		//
		// Ok so I should have used a properties file... but I need to learn how!
/*		inputFilePath = FileSystems.getDefault().getPath("/Users/mark/Documents/Bootcamp/CapstoneProject/finefoods.txt");
		outputFilePath = FileSystems.getDefault().getPath("/Users/mark/Documents/Bootcamp/CapstoneProject/finefoods.out");
		tableFilePath = FileSystems.getDefault().getPath("/Users/mark/Documents/Bootcamp/CapstoneProject/finefoods_table.cql");
		keyspaceFilePath = FileSystems.getDefault().getPath("/Users/mark/Documents/Bootcamp/CapstoneProject/finefoods_keyspace.cql");
*/		
		if (args.length != 8){
			System.out.println("Wrong number of arguments, useage:\n <path> <keyspace cql> <table1 cql> <table2 cql> <source data> <dry run (true|false)> <limit count> <node ip>"
					+ "\n\nNote: a limit of 0 means import all");
			System.exit(0);;
		}
		
		String inputPath = args [0];
		String keyspaceFile = args [1];
		String tableFile = args [2];
		String table2File = args [3];
		String dataFile = args [4];
		String dryrun = args [5]; // false if not a dry run otherwise it will be
		String limitstr = args[6]; // limit count
		String nodeip = args [7];
				
		inputFilePath = FileSystems.getDefault().getPath(inputPath + "/" + dataFile);
		outputFilePath = FileSystems.getDefault().getPath(inputPath + "/" + "output.out");
		tableFilePath = FileSystems.getDefault().getPath(inputPath + "/" + tableFile);
		table2FilePath = FileSystems.getDefault().getPath(inputPath + "/" + table2File);
		keyspaceFilePath = FileSystems.getDefault().getPath(inputPath + "/" + keyspaceFile);
		
		LoadData loaddata = new LoadData();
		//loaddata.makeCsvFile(inputFilePath, outputFilePath); // comment this in to spit out a file (its a bit of a hack, so be warned)
		loaddata.setupConnection(nodeip);
		loaddata.createKeyspaceOrTable(keyspaceFilePath);
		loaddata.createKeyspaceOrTable(tableFilePath);
		loaddata.createKeyspaceOrTable(table2FilePath);
		if (dryrun.matches("false")){ // load data into db if the dryrun = false
			loaddata.readSourceFile(inputFilePath, outputFilePath, limitstr, nodeip);
		}
		loaddata.cleanupConnection();

	}
	
	private void setupConnection(String nodeip){
		node = nodeip;
		client.connect(node);
	}

	private void cleanupConnection(){
		client.close();
	}

		/**
		 * Reads the raw data from a file
		 * 
		 * @param filePath
		 */
		public void readSourceFile(Path inputFilePath, Path outputFilePath, String limitstr, String nodeip){
			BufferedReader reader;
			try {
				// connect the session here, otherwise if you do it further down where
				// we call the insert, then you get a "too many open files" error
				// as the connections stack up and it goes wonky
				Integer limitcount = 0;
				limit = Integer.parseInt(limitstr);
				//InputStream iptStream = Files.newInputStream(inputFilePath); // setup the file stream
				//reader = new BufferedReader(new InputStreamReader(iptStream));
				String file = inputFilePath.toString();
				reader = new BufferedReader(new FileReader(file));
				String line = null; // raw line date
				String optLine [] = null; // split text container
				while ((line = reader.readLine()) != null) { // loop until the end of the file
					if (line.contains(": ")){
						optLine = line.split(": "); // split the text with ":"
						formatDataForOutput(optLine[0].trim(), optLine[1].trim()); // trim gets rid of leading and trailing chaff
						formatDataForOutput2(optLine[0].trim(), optLine[1].trim()); // trim gets rid of leading and trailing chaff
					}
					else { 
						// if the line has no ":" then its assumed to be blank, I guess we should have tested for a blank line too
						// but it could be blank with some spaces and that would cause a headache
						limitcount ++;
						if (limitcount > limit && limit > 0) {
							System.out.println("limit of " + limit + " reached.");
							break;
						}
						makeKeyValueLists(); // makes the lists that hold the values
						makeKeyValueLists2(); // makes the lists that hold the values
						node = nodeip;
						client.runQuery("use finefoods"); // select the right keyspace, there's probably a neater way to do this when you insert
						client.insert(table, setkeys, values); // insert using the list sets
						client.insert(table2, setkeys2, values2); // insert using the list sets
						
						keyvalues.clear(); // its important to clear this lot out here
						keyvalues2.clear(); // its important to clear this lot out here
						setkeys.clear();
						values.clear();
						setkeys2.clear();
						values2.clear();
						System.out.print("\rgood records = " + tb1count + ", bad records: "+ tb1err);
					}
				}
				System.out.println("Complete: good records = " + tb1count + ", bad records: "+ tb1err);

			} catch (IOException x) {
				x.printStackTrace();
			}
			
		}

		/**
		 * Formats the raw data and loads into Map. All the values are loaded into a class-wide map
		 * this map is then used to hold the values until they are copied out into lists. Its a bit overkill
		 * but I done it like this as I might need to re-use this someday :-)
		 * 
		 * @param lineKey
		 * @param lineValue
		 */
		private void formatDataForOutput(String lineKey, String lineValue){
			String key;
			String value;
			if (lineKey.matches("review/time")){
				key = "date";
				value = getDateFromTsString(lineValue);
				keyvalues.put(key, value);
				valueTypes.put(key,"text");
				key = "time";
				value = String.valueOf(getTimestampFromTsString(lineValue).getTime());
				keyvalues.put(key, value);
				valueTypes.put(key,"timestamp");
			}
			else if (lineKey.matches("product/productId")) { 
				key = "prod_id";
				value = lineValue;
				keyvalues.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/userId")) {
				key = "user_id";
				value = lineValue;
				keyvalues.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/profileName")) {
				key = "profile_name";
				value = lineValue;
				keyvalues.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/helpfulness")) {
				key = "helpfulness";
				value = lineValue;
				keyvalues.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/score")) {
				key = "score";
				value = lineValue;
				keyvalues.put(key, value);
				valueTypes.put(key,"double");
			}
			else if (lineKey.matches("review/summary")) {
				key = "summary";
				value = lineValue;
				keyvalues.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/text")) {
				key = "text";
				value = lineValue;
				keyvalues.put(key, value);
				valueTypes.put(key,"text");
			}	
		}
		
		/**
		 * Formats the raw data and loads into Map. All the values are loaded into a class-wide map
		 * this map is then used to hold the values until they are copied out into lists. Its a bit overkill
		 * but I done it like this as I might need to re-use this someday :-)
		 * 
		 * @param lineKey
		 * @param lineValue
		 */
		private void formatDataForOutput2(String lineKey, String lineValue){
			String key;
			String value;
			if (lineKey.matches("review/time")){
				key = "hour";
				value = getTimeFromTsString(lineValue);
				keyvalues2.put(key, value);
				valueTypes.put(key,"text");
				key = "time";
				value = String.valueOf(getTimestampFromTsString(lineValue).getTime());
				keyvalues2.put(key, value);
				valueTypes.put(key,"timestamp");
			}
			else if (lineKey.matches("product/productId")) { 
				key = "prod_id";
				value = lineValue;
				keyvalues2.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/userId")) {
				key = "user_id";
				value = lineValue;
				keyvalues2.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/profileName")) {
				key = "profile_name";
				value = lineValue;
				keyvalues2.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/helpfulness")) {
				key = "helpfulness";
				value = lineValue;
				keyvalues2.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/score")) {
				key = "score";
				value = lineValue;
				keyvalues2.put(key, value);
				valueTypes.put(key,"double");
			}
			else if (lineKey.matches("review/summary")) {
				key = "summary";
				value = lineValue;
				keyvalues2.put(key, value);
				valueTypes.put(key,"text");
			}
			else if (lineKey.matches("review/text")) {
				key = "text";
				value = lineValue;
				keyvalues2.put(key, value);
				valueTypes.put(key,"text");
			}	
		}

		/**
		 * We have two lists, one holds key names and one holds values. These come from the class-wide Map
		 * created in formatDataForOutput(). The list holiding the key names is just a list of strings. The
		 * list holding the values could be a mixed bag of object types like timestamps or such. This is
		 * why we have to load the Map into two seperate lists.
		 * 
		 * It would probably have been easier to just use two lists from the get-go, but using one Map makes
		 * it easier to keep things in order until you extract it
		 * 
		 */
		public void makeKeyValueLists(){
			Integer pointer = 0;
			Date date;
			Double dbl;
			
			// Load the key names (strings)
			// As they are all strings they just copy
			for (Object obj : keyvalues.keySet()){
				setkeys.add(obj.toString());
			}

			// Copy the types into an array
			String types[] = new String[setkeys.size()];
			types = setkeys.toArray(types);

			// Load the values (objects)
			// These could be a mixed bag so we have to check
			for (String obj: keyvalues.values()){
				
				if (types[pointer].matches("time")) { // add object of given type
						date = new Date(Long.parseLong(obj));
						values.add(date);
				}
				else if (types[pointer].matches("score")) { // add object of given type
						dbl = Double.parseDouble(obj);
						values.add(dbl);
				} 
				else { // default is text
					values.add(obj);
				}
					
				pointer ++;
			}
		}

		/**
		 * We have two lists, one holds key names and one holds values. These come from the class-wide Map
		 * created in formatDataForOutput(). The list holiding the key names is just a list of strings. The
		 * list holding the values could be a mixed bag of object types like timestamps or such. This is
		 * why we have to load the Map into two seperate lists.
		 * 
		 * It would probably have been easier to just use two lists from the get-go, but using one Map makes
		 * it easier to keep things in order until you extract it
		 * 
		 */
		public void makeKeyValueLists2(){
			Integer pointer = 0;
			Date date;
			Double dbl;
			
			// Load the key names (strings)
			// As they are all strings they just copy
			for (Object obj : keyvalues2.keySet()){
				setkeys2.add(obj.toString());
			}

			// Copy the types into an array
			String types[] = new String[setkeys2.size()];
			types = setkeys2.toArray(types);

			// Load the values (objects)
			// These could be a mixed bag so we have to check
			for (String obj: keyvalues2.values()){
				
				if (types[pointer].matches("time")) { // add object of given type
						date = new Date(Long.parseLong(obj));
						values2.add(date);
				}
				else if (types[pointer].matches("score")) { // add object of given type
						dbl = Double.parseDouble(obj);
						values2.add(dbl);
				} 
				else { // default is text
					values2.add(obj);
				}
					
				pointer ++;
			}
		}

		// Might not be used
		public void makeCsvFile(Path inputFilePath, Path outputFilePath) {
			BufferedReader reader;
			try {
				InputStream iptStream = Files.newInputStream(inputFilePath);
				writer = Files.newBufferedWriter(outputFilePath,Charset.defaultCharset());
				reader = new BufferedReader(new InputStreamReader(iptStream));
				String line = null; // raw line date
				String optLine [] = null; // split text
				while ((line = reader.readLine()) != null) {
					if (line.contains(":")){
						optLine = line.split(": "); // split the text with ":"
						outputDataToFile(optLine[0], optLine[1], outputFilePath);
					}
					else {
						outputDataToFile("end", "line", outputFilePath);
					}
				}
			} catch (IOException x) {
				x.printStackTrace();
			}
			
		}

		// Not used, but keeping for reference
		public void CrunchDataOld(Path inputFilePath, Path outputFilePath){
			BufferedReader reader;
			BufferedWriter writer;
			try {
				InputStream iptStream = Files.newInputStream(inputFilePath);
				reader = new BufferedReader(new InputStreamReader(iptStream));
				writer = Files.newBufferedWriter(outputFilePath,Charset.defaultCharset());
				String line = null; // raw line date
				String optLine [] = null; // split text
				while ((line = reader.readLine()) != null) {
					if (line.contains(":")){
						optLine = line.split(":"); // split the text with ":"
						writer.write(optLine[1].trim() + "|");
						writer.flush();
					}
					else
						writer.newLine();
				}
				writer.close();
			} catch (IOException x) {
				x.printStackTrace();
			}
			
		}

		// Might not be used
		private void outputDataToFile(String lineKey, String lineValue, Path outputFilePath) throws IOException{
			String value;
			if (lineKey.matches("review/time")){ // timestamp and date
				value = getDateFromTsString(lineValue);
				csvLine = csvLine + value + delimiter;
				value = String.valueOf(getTimestampFromTsString(lineValue).getTime());
				csvLine = csvLine + value + delimiter;
			}
			else if (lineKey.matches("product/productId")) {
				value = lineValue;
				csvLine = csvLine + value + delimiter;
			}
			else if (lineKey.matches("review/userId")) {
				value = lineValue;
				csvLine = csvLine + value + delimiter;
			}
			else if (lineKey.matches("review/profileName")) {
				value = lineValue;
				csvLine = csvLine + value + delimiter;
			}
			else if (lineKey.matches("review/helpfulness")) {
				value = lineValue;
				csvLine = csvLine + value + delimiter;
			}
			else if (lineKey.matches("review/score")) {
				value = lineValue;
				csvLine = csvLine + value + delimiter;
			}
			else if (lineKey.matches("review/summary")) {
				value = lineValue;
				csvLine = csvLine + value + delimiter;
			}
			else if (lineKey.matches("review/text")) {
				value = lineValue;
				csvLine = csvLine + value; // no delimiter, last line
			}
			else  if (lineKey.matches("end")){
				System.out.println(csvLine);
				writer.write(csvLine);
				writer.write("\n");
				writer.flush();
				csvLine = "";
			}
	
		}

		
		/**
		 * Creates the table(s) needed for the project
		 * 
		 * @param inputFilePath
		 * @param node
		 */
		// this method needs a re-visit as it didnt work too well for some odd reason
		public void createKeyspaceOrTable(Path inputFilePath){
			// Creates the table(s) needed for the project work
			// There is no syntax checking!
			BufferedReader reader;
			try {
				InputStream iptStream = Files.newInputStream(inputFilePath);
				reader = new BufferedReader(new InputStreamReader(iptStream));
				String line = null;
				String statement="";
				while ((line = reader.readLine()) != null) {
					statement = statement + line;
					}
				System.out.println(statement);
				client.runQuery(statement);
			} catch (IOException x) {
				x.printStackTrace();
			}
			
			
		}
		
		/**
		 * Creates a date from a timestamp string
		 * 
		 * @param ts
		 * @return
		 */
		private String getDateFromTsString(String ts){
			Date timestamp;
			SimpleDateFormat date;
			String myDate;
			timestamp = new Date(Long.parseLong(ts)*1000); /// have to x by 1000 because expects in millisecs
			date = new SimpleDateFormat("YYYY-MM-dd");
			myDate = date.format(timestamp);
			return myDate;
		}

		private String getTimeFromTsString(String ts){
			Date timestamp;
			SimpleDateFormat date;
			String myTime;
			timestamp = new Date(Long.parseLong(ts)*1000); /// have to x by 1000 because expects in millisecs
			date = new SimpleDateFormat("HH:mm:ss");
			myTime = date.format(timestamp);
			return myTime;
		}
		
		private Date getTimestampFromTsString(String ts){
			Date timestamp;
			timestamp = new Date(Long.parseLong(ts)*1000); /// have to x by 1000 because expects in millisecs
			return timestamp;
		}

}
