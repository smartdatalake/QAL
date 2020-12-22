package definition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.*;
public class ProteusJDBC {
    public static void getCSVfromProteus (String tableName,String pathToSaveCSV) throws Exception{
        System.out.println("fetching table from proteus "+tableName);
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://diascld32.iccluster.epfl.ch:18007;serialization=PROTOBUF", "sdlhshah", "Shah13563556");
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from "+tableName+" limit "+Paths.JDBCRowLimiter());
        ResultSetMetaData rsmd = rs.getMetaData();
        String header = "";
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            header += rsmd.getColumnName(i) + ",";
        header = header.substring(0, header.length() - 1);
        int numberOfColumns = rsmd.getColumnCount();
        BufferedWriter writer = new BufferedWriter(new FileWriter(pathToSaveCSV+"/"+tableName+".csv"));
        writer.write(header + "\n");
        while (rs.next()) {
            String row = "";
            for (int i = 1; i <= numberOfColumns; i++) {
                String value = rs.getString(i);
                if (rs.getObject(i) != null)
                    row += value.replace(',', ' ');
                if (i < numberOfColumns)
                    row += ',';
            }
            writer.write(row + "\n");
        }
        writer.close();
        System.out.println("fetched table from proteus "+tableName);
    }
}
