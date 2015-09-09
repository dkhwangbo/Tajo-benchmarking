
import org.apache.tajo.client.v2.TajoClient;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.QueryFailedException;
import org.apache.tajo.exception.QueryKilledException;
import org.apache.tajo.exception.TajoException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TajoBenchmark {

    public static void run(String hostname, int port, String dataBase, String sql) throws ClientUnableToConnectException {
        try (TajoClient client = new TajoClient(hostname, port)) {
            try {
                int total = 1;
                int pre = 0, cur =0;
                client.selectDB(dataBase);
                ResultSet result = client.executeQuery(sql);
                result.next();

                while(result.next()) {
                    total++;
                    cur = result.getInt("l_orderkey");

                    if(pre-cur < 0)
                        System.out.println("ERROR in " + total + " row! PRE = " + pre + ", CUR = " + cur);

                    pre = cur;
                }

                System.out.println("Total row : " + Integer.toString(total));

            } catch (QueryFailedException e) {
                System.err.println("query is failed.");
            } catch (QueryKilledException e) {
                System.err.println("query is killed.");
            } catch (SQLException | TajoException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws ClientUnableToConnectException {
        String hostname = "localhost";
        int port = 26002;
        String dataBase = "tpch";
        String sql = "SELECT * FROM LINEITEM ORDER BY L_ORDERKEY DESC;";

        run(hostname, port, dataBase, sql);
    }
}