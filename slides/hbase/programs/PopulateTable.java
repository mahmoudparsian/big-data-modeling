import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class PopulateTable {
    
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


      // Instantiating Put class
      // accepts a row name.
      Put p = new Put(Bytes.toBytes("row100")); // rowID

      // adding values using add() method
      // accepts column family name, qualifier/row name ,value
      p.addColumn(
              Bytes.toBytes("c1"),    // CF
              Bytes.toBytes("name"),  // Qualifier
              Bytes.toBytes("raju")); // Value

      p.addColumn(
              Bytes.toBytes("c1"),
              Bytes.toBytes("city"),
              Bytes.toBytes("hyderabad"));

      p.addColumn(
              Bytes.toBytes("c2"),
              Bytes.toBytes("designation"), 
              Bytes.toBytes("manager"));

      p.addColumn(
              Bytes.toBytes("c2"),
              Bytes.toBytes("salary"), 
              Bytes.toBytes("50000"));
      
      
      Put p2 = new Put(Bytes.toBytes("row200")); // rowID

      // adding values using add() method
      // accepts column family name, qualifier/row name ,value
      p2.addColumn(
            Bytes.toBytes("c1"), 
            Bytes.toBytes("name22"),
            Bytes.toBytes("raju444"));
      
      p2.addColumn(
            Bytes.toBytes("c1"), 
            Bytes.toBytes("education"),
            Bytes.toBytes("business"));
      
      p2.addColumn(
            Bytes.toBytes("c1"),
            Bytes.toBytes("city333"),
            Bytes.toBytes("hyderabadrrr4444"));

      p2.addColumn(
            Bytes.toBytes("c2"),
            Bytes.toBytes("designation4444"), 
            Bytes.toBytes("manager4444"));

      p2.addColumn(
            Bytes.toBytes("c2"),
            Bytes.toBytes("salary"), 
            Bytes.toBytes("50000"));  
      
      p2.addColumn(
            Bytes.toBytes("c2"),
            Bytes.toBytes("age"), 
            Bytes.toBytes("20"));            
      // Saving the put Instance to the HTable.
     table.put(p);
     table.put(p2);
     System.out.println("data inserted");
      
      // closing HTable
      table.close();
   }
}

