import java.io.*;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JDBCCustomConnector {

    public static void main(String[] args) {
        if( args.length < 4 ) {
            System.out.println("Invalid number of arguments passed, required 4 but received: "+args.length);
            System.out.println("[Command Usage] java -cp ojdbc8.jar:sqljdbc4-4.0.jar:. JDBCCustomConnector <DBUserName> <DBPassword> <DBURL> <DBQueryFile> [QueryParameters]");
            System.exit(1);
        }

        String username = args[0].trim();
        String passcode = args[1].trim();
        String dbURL = args[2].trim();
        String filename = args[3].trim();

        System.out.print("DB URL: {"+dbURL+"} : ");

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(dbURL,username,passcode);
            System.out.println("Connection successful.");

            BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
            StringBuilder query = new StringBuilder();
            String s;
            while ( (s= br.readLine()) != null ) query.append(s.trim());

            PreparedStatement statement = connection.prepareStatement(query.toString());

//            System.out.println(""+((OraclePreparedStatementWrapper) statement).getOriginalSql());
            PrepraredHelper prepHelper = new PrepraredHelper(statement);

            for(int i = 1; i <= args.length - 4; i ++) {
                prepHelper.setString(i, args[i+3].trim());
            }

            try {
                Pattern pattern = Pattern.compile("\\?");
                Matcher matcher = pattern.matcher(query.toString());
                StringBuffer sb = new StringBuffer();
                int indx = 1;  // Parameter begin with index 1
                while (matcher.find()) {
                    matcher.appendReplacement(sb, prepHelper.getParameter(indx++));
                }
                matcher.appendTail(sb);
                System.out.println("Executing Query [" + sb.toString() + "] with Database[");
            } catch (Exception ex) {
                System.out.println("Executing Query [" + query.toString() + "] with Database[");
            }


            ResultSet resultSet = prepHelper.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnsNumber = resultSetMetaData.getColumnCount();
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                System.out.print(resultSetMetaData.getColumnName(i));
            }
            System.out.println();

            while (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                 columnsNumber = metaData.getColumnCount();
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    System.out.println(columnValue+ " && "+metaData.getColumnLabel(i).trim());
                    System.out.print(columnValue);
                }
                System.out.println();
            }

        } catch (SQLException e) {
            System.err.println("Error occurred due to: "+e);
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            System.err.println("Query file not found."+e);
        } catch (IOException e) {
            System.err.println("Error occurred while reading query file."+e);
        } finally {
            try {
                if( connection != null ) {
                    connection.close();
                }
            } catch (SQLException e) {
                System.err.println("Error occurred while closing connection: "+e);
            }
        }


    }
}

