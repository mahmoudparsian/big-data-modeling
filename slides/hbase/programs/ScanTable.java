import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;


public class ScanTable{

    public static Configuration createHadoopConfiguration(String zookeeperQuorum) {
        Configuration conf = HBaseConfiguration.create();
        //
        // will parameterize and configure the HBase server
        //
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);
        //
        return conf;
    }
    
    public static void main(String[] args) throws Exception {

      // Instantiating Configuration class
      String zookeeperQuorum = "localhost";
      Configuration conf = createHadoopConfiguration(zookeeperQuorum);
      //
      Connection conn = ConnectionFactory.createConnection(conf);      
      //
      Table table = conn.getTable(TableName.valueOf("emp"));


      // Instantiating the Scan class
      Scan scan = new Scan();

      // Scanning the required columns
      scan.addColumn(
              Bytes.toBytes("c1"), // CF
              Bytes.toBytes("education")); // Qualifier
      
      scan.addColumn(
              Bytes.toBytes("c2"), // CF
              Bytes.toBytes("salary")); // Qualifier
      
      //scan.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("stocks"));
      //scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("address"));

      // Getting the scan result
      ResultScanner scanner = table.getScanner(scan);

      // Reading values from scan result
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        System.out.println("Found row : " + result); 
      
        byte[] educationAsByteArray = result.getValue(Bytes.toBytes("c1"),Bytes.toBytes("education"));

        byte[]  salaryAsByteArray = result.getValue(Bytes.toBytes("c2"),Bytes.toBytes("salary"));

        // Printing the values
        String education = Bytes.toString(educationAsByteArray);
        String salary = Bytes.toString(salaryAsByteArray);
      
        System.out.println("education: " + education);        
        System.out.println("salary: " + salary);        
       
      }
      
      //closing the scanner
      scanner.close();
   }
}