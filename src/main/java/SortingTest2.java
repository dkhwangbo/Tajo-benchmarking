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

public class SortingTest2 {
    public void run(TajoClient client, String sql, String column1, String column2) {
        int flag1 = 1, flag2 = 1;
        long numRows = 0;
        long pre1 = 0, cur1;
        long pre2 = 0, cur2;

        System.out.println("RUN!");
        try {
            ResultSet res = client.executeQuery(sql);
            System.out.println("executeQuery is done! Verification start!");

            while(res.next()) {
                if(numRows == 0) {
                    pre1 = res.getLong(column1);
                    pre2 = res.getLong(column2);
                }

                numRows++;
                cur1 = res.getLong(column1);

                if(pre1 == cur1) {
                    cur2 = res.getLong(column2);
                    if(flag2 == 1) {
                        if(pre2 > cur2)
                            flag2 = 0;
                        else if(pre2 < cur2)
                            flag2 = 2;
                    }
                    else if(((flag2 == 0) && (pre2 < cur2)) || ((flag2 == 2) && (pre2 > cur2)))
                        System.out.println(column2 + " ERROR! current row=" + numRows + ", pre = " + pre2 + ", cur = " + cur2);
                }

                if(flag1 == 1) {
                    if(pre1 > cur1)
                        flag1 = 0;
                    else if(pre1 < cur1)
                        flag1 = 2;
                }
                else if(((flag1 == 0) && (pre1 < cur1)) || ((flag1 == 2) && (pre1 > cur1)))
                    System.out.println(column1 + " ERROR! current row=" + numRows + ", pre = " + pre1 + ", cur = " + cur1);

                pre1 = cur1;
                pre2 = res.getLong(column2);
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

            System.out.print("key1 : ");
            String key1 = s.nextLine();
            System.out.print("key2 : ");
            String key2 = s.nextLine();

            run(client, query, key1, key2);

            System.out.print("Another test? (y/n) : ");
            if(s.nextLine().equals("n"))
                break;
        }

        client.close();

        return 0;
    }

    public static void main(String args[]) throws ClientUnableToConnectException, IOException, UndefinedDatabaseException {
        SortingTest2 t = new SortingTest2();
        System.exit(t.Driver());
    }
}