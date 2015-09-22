import java.io.IOException;
import org.apache.tajo.client.v2.TajoClient;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.QueryFailedException;
import org.apache.tajo.exception.QueryKilledException;
import org.apache.tajo.exception.UndefinedDatabaseException;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.List;

public class TajoBenchmark {

    public void run(TajoClient client, String fileName, String sql) {
        System.out.print(fileName + " ---------- ");

        try {
            List<String> query = Arrays.asList(sql.split(";"));
            Iterator<String> i = query.iterator();
            int count = 0;

            long startTime = System.currentTimeMillis();
            while(true) {
                count++;
                client.executeQuery(i.next() + ";");
                if(count == query.size()-1) {
                    break;
                }
            }
            long duration = System.currentTimeMillis() - startTime;

            NumberFormat formatter = new DecimalFormat("#0.00000");
            System.out.print("QUERY SUCCESS! Execution time : " + formatter.format(duration / 1000d) + " seconds");
        } catch (QueryFailedException e) {
            System.err.print("FAILED.");
        } catch (QueryKilledException e) {
            System.err.print("KILLED.");
        } catch (Exception e) {
            System.err.println("OTHER EXCEPTION.");
            e.printStackTrace();
        }
        System.out.println();
    }

    public int Driver() throws ClientUnableToConnectException, IOException, UndefinedDatabaseException {
        Scanner s = new Scanner(System.in);

        System.out.print("hostname : ");
        String hostname = s.nextLine();
        System.out.print("port : ");
        int port = Integer.valueOf(s.nextLine());
        TajoClient client = new TajoClient(hostname, port);

        System.out.print("database : ");
        client.selectDB(s.nextLine());

        System.out.print("query directory : ");
        File folder = new File(s.nextLine());
        
        File[] listOfFiles = folder.listFiles();
        Arrays.sort(listOfFiles);
        System.out.println("\nSTARTING! total : " + listOfFiles.length);
        for (File file : listOfFiles) {
            if (file.isFile()) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                try {
                    StringBuilder sb = new StringBuilder();
                    String line = br.readLine();

                    while (line != null) {
                        sb.append(line);
                        sb.append(" ");
                        line = br.readLine();
                    }

                    run(client, file.getName(), sb.toString());
                } finally {
                    br.close();
                }
            }
        }
        System.out.println("ALL TEST IS DONE");
        client.close();

        return 0;
    }

    public static void main(String args[]) throws ClientUnableToConnectException, IOException, UndefinedDatabaseException {
        TajoBenchmark t = new TajoBenchmark();
        System.exit(t.Driver());
    }
}