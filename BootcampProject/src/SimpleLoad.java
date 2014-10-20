import java.util.Date;
import java.util.Calendar;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

//import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
//import com.datastax.driver.core.Row;
//import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class SimpleLoad {
    private Cluster cluster;
    private Session session;

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public void close() {
        cluster.close();
    }

    public void createSchema() {
        session.execute("CREATE KEYSPACE IF NOT EXISTS bootcamp WITH replication " +
                "= {'class' : 'SimpleStrategy', 'replication_factor' : 1};");

        session.execute("USE bootcamp;");

        session.execute("DROP TABLE IF EXISTS reviews_by_day;");

        session.execute(
                "CREATE TABLE reviews_by_day ( " +
                        "id uuid, " +
                        "productId text, " +
                        "userId text, " +
                        "profileName text, " +
                        "helpfulness text, " +
                        "score double, " +
                        "time timestamp, " +
                        "summary text, " +
                        "review text, " +
                        "day text, " +
                        "PRIMARY KEY(day, userId, productID) " +
                        "); " // WITH CLUSTERING ORDER BY (dep_time ASC);"
        );

    }

    public void loadData() {
        String csvFile = "/home/student/Bootcamp/finefoods.txt";
        BufferedReader br = null;
        int i, iGood, iBad;
        String line = "";
        String csvSplitBy = ": ";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdfDay = new SimpleDateFormat("yyyy-MM-dd");

        long MINUTE = 60 * 1000;
        long HOUR = 60 * MINUTE;

        UUID id = null;
        String productId = null;
        String userId = null;
        String profileName = null;
        String helpfulness = null;
        Double score = null;
        Date time = null;
        String summary = null;
        String review = null;
        String day = null;

        String queryText = "INSERT INTO reviews_by_day (" +
                "id, " +
                "productId, " +
                "userId, " +
                "profileName, " +
                "helpfulness, " +
                "score, " +
                "time, " +
                "summary, " +
                "review, " +
                "day) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        PreparedStatement preparedStatement = session.prepare(queryText);

        try {

            System.out.println("Start: " + sdf.format(new Date()));

            br = new BufferedReader(new FileReader(csvFile));
            i = 1;
            iGood = 0;
            iBad = 0;
            while ((line = br.readLine()) != null && i <= 10000000) {

                // use comma as separator
                String[] fields = line.split(csvSplitBy, 2);

                if (fields[0].toLowerCase().equals("product/productid")) {
                    productId = fields[1];
                } else if (fields[0].toLowerCase().equals("review/userid")) {
                    userId = fields[1];
                } else if (fields[0].toLowerCase().equals("review/profilename")) {
                    profileName = fields[1];
                } else if (fields[0].toLowerCase().equals("review/helpfulness")) {
                    helpfulness = fields[1];
                } else if (fields[0].toLowerCase().equals("review/score")) {
                    score = Double.parseDouble(fields[1]);
                } else if (fields[0].toLowerCase().equals("review/time")) {
                    time = new Date(Long.parseLong(fields[1])*1000L);
                } else if (fields[0].toLowerCase().equals("review/summary")) {
                    summary = fields[1];
                } else if (fields[0].toLowerCase().equals("review/text")) {
                    review = fields[1];
                } else {

                    if (time != null && productId != null && userId != null) {
                        iGood++;

                        id = UUID.randomUUID();
                        // Calendar is the work horse fot the day key value
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(time);
                        // Set the time to zero to get to just the date
                        cal.set(Calendar.HOUR_OF_DAY, 0);
                        cal.set(Calendar.MINUTE, 0);
                        cal.set(Calendar.SECOND, 0);
                        cal.set(Calendar.MILLISECOND, 0);
                        // Create the new date object
                        day = sdfDay.format(cal.getTime());
                        //day = sdfDay.parse(sdfDay.format(time));

                        session.execute(preparedStatement.bind(id, productId, userId, profileName, helpfulness, score, time, summary, review, day));
                    } else {
                        iBad++;
                        System.out.println("***** Failed Record " + iBad + " *****");
                        System.out.println("ProductId:\t\t" + productId);
                        System.out.println("userId:\t\t" + userId);
                        System.out.println("profileName:\t\t" + profileName);
                        System.out.println("helpfulness:\t\t" + helpfulness);
                        System.out.println("score:\t\t" + score);
                        System.out.println("time:\t\t" + time);
                        System.out.println("summary:\t\t" + summary);
                        System.out.println("\n");
                    }

                    // reset all values on a new record
                    productId = null;
                    userId = null;
                    profileName = null;
                    helpfulness = null;
                    score = null;
                    time = null;
                    summary = null;
                    review = null;
                    day = null;
                }

                //System.out.println("id:" + id + " , fl_date:" + fl_date.toString() + " , dep_time:" + dep_time.toString());
                i++;
            }
            System.out.println("Stop: " + sdf.format(new Date()));
            System.out.println("Read " + i + " lines.");
            System.out.println("Encountered " + iBad + " bad records.");
            System.out.println("Inserted " + iGood + " records.");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        //} catch (ParseException e) {
        //    e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("Done");

    }

    public static void main(String[] args) {
        SimpleLoad client = new SimpleLoad();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.close();
    }
}