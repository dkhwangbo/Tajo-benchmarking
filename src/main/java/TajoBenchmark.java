import org.apache.tajo.client.v2.TajoClient;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.QueryFailedException;
import org.apache.tajo.exception.QueryKilledException;
import org.apache.tajo.exception.TajoException;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.List;

public class TajoBenchmark {

    public void run(String hostname, int port, String dataBase, String fileName, String sql) throws ClientUnableToConnectException {
        System.out.print(fileName + " ---------- ");

        try (TajoClient client = new TajoClient(hostname, port)) {
            try {
                client.selectDB(dataBase);
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
            } finally {
                client.close();
            }
        }

        System.out.println();
    }

    public int Driver() throws ClientUnableToConnectException, IOException {
        Scanner s = new Scanner(System.in);

        System.out.print("hostname : ");
//        String hostname = "localhost";
        String hostname = s.nextLine();
        System.out.print("port : ");
        int port = s.nextInt();
//        int port = 26002;
        System.out.print("database : ");
//        String dataBase = "tpch";
        String dataBase = s.nextLine();

        System.out.print("query directory : ");

//        File folder = new File(s.nextLine());
        File folder = new File("/home/dk/Benchmarking/tajo/tpc-h/query_revised");
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

                    run(hostname, port, dataBase, file.getName(), sb.toString());
                } finally {
                    br.close();
                }
            }
        }
        System.out.println("ALL TEST IS DONE");

        return 0;
    }

    public static void main(String args[]) throws ClientUnableToConnectException, IOException {
        TajoBenchmark t = new TajoBenchmark();
        System.exit(t.Driver());
    }
}