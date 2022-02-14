import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadData{

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


      String rowID = args[0];
      System.out.println("rowID="+rowID);
      
      // Instantiating Get class
      Get g = new Get(Bytes.toBytes(rowID)); // rowID

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte[] nameAsByteArray = 
              result.getValue(Bytes.toBytes("c1"),Bytes.toBytes("name"));

      byte[]  ageAsByteArray = 
              result.getValue(Bytes.toBytes("c2"),Bytes.toBytes("age"));

      // Printing the values
      String name = Bytes.toString(nameAsByteArray);
      String age = Bytes.toString(ageAsByteArray);
      
      System.out.println("name: " + name);      
      System.out.println("age: " + age);

   }
}