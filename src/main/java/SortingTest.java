import org.apache.tajo.client.v2.TajoClient;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.QueryFailedException;
import org.apache.tajo.exception.QueryKilledException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UndefinedDatabaseException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class SortingTest {
    public void run(TajoClient client, String sql, String column) {
        int flag = 1;
        long numRows = 0;
        long pre = 0, cur;

        System.out.println("RUN!");
        try {
            ResultSet res = client.executeQuery(sql);
            System.out.println("executeQuery is done! Verification start!");

            while(res.next()) {
                if(numRows == 0)
                    pre = res.getLong(column);

                numRows++;
                cur = res.getInt(column);

                if(numRows == 1)
                    pre = cur;

                if(flag == 1) {
                    if(pre > cur)
                        flag = 0;
                    else if(pre < cur)
                        flag = 2;
                }
                else if(((flag == 0) && pre < cur) || ((flag == 2) && pre > cur))
                    System.out.println("ERROR! current row=" + numRows + ", pre = " + pre + ", cur = " + cur);

                pre = cur;
            }
            System.out.println("Done! Total row = " + numRows);
        } catch (QueryFailedException e) {
            System.err.println("query is failed.");
        } catch (QueryKilledException e) {
            System.err.println("query is killed.");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (TajoException e) {
            e.printStackTrace();
        }
    }

    public int Driver() throws ClientUnableToConnectException, IOException, UndefinedDatabaseException {
        Scanner s = new Scanner(System.in);

        System.out.print("hostname : ");
        String hostname = s.nextLine();
        System.out.print("port : ");
        int port = Integer.valueOf(s.nextLine());
        TajoClient client = new TajoClient(hostname, port);

        while(true) {
            System.out.print("database : ");
            client.selectDB(s.nextLine());

            System.out.print("query : ");
            String query = s.nextLine();

            System.out.print("key : ");
            String key = s.nextLine();

            run(client, query, key);

            System.out.print("Another test? (y/n)");
            if(s.nextLine().equals("n"))
                break;
        }

        client.close();

        return 0;
    }

    public static void main(String args[]) throws ClientUnableToConnectException, IOException, UndefinedDatabaseException {
        SortingTest t = new SortingTest();
        System.exit(t.Driver());
    }
}