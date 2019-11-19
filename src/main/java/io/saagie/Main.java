package io.saagie;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;


public class Main {

    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static String username;
    private static String password;
    private static String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");

    private static LoginContext kinit(String username, String password) throws LoginException {
        LoginContext lc = new LoginContext(Main.class.getSimpleName(), callbacks -> {
            for (Callback c : callbacks) {
                if (c instanceof NameCallback)
                    ((NameCallback) c).setName(username);
                if (c instanceof PasswordCallback)
                    ((PasswordCallback) c).setPassword(password.toCharArray());
            }
        });
        lc.login();
        return lc;
    }

    public static void main(String[] args) throws LoginException, IOException, ClassNotFoundException, SQLException {
        if (args.length > 0) { // If any arguments provided
            username = args[0];
            password = args[1];
        } else {
            System.out.println("Usage : java -jar <username> <password>");
            System.exit(0);
        }

        URL url = Main.class.getClassLoader().getResource("jaas.conf");
        System.setProperty("java.security.auth.login.config", url != null ? url.toExternalForm() : null);

        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"));
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/hdfs-site.xml"));

        UserGroupInformation.setConfiguration(conf);

        LoginContext lc = kinit(username, password);
        UserGroupInformation.loginUserFromSubject(lc.getSubject());
        String kerberosRealm = UserGroupInformation.getLoginUser().getUserName().split("@")[1];

        //HDFS
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path("/tmp/test.txt");
        FSDataOutputStream outputStream = fs.create(filePath);
        outputStream.writeBytes("Kerberos");
        outputStream.close();

        FSDataInputStream inputStream = fs.open(filePath);
        String out = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        System.out.println(out);
        inputStream.close();
        fs.close();

        Class.forName(JDBC_DRIVER_NAME);
        // Hive
        Connection hiveConnection = null;
        hiveConnection = DriverManager.getConnection("jdbc:hive2://nn1:10000/;principal=hive/_HOST@" + kerberosRealm + ";saslQop=auth-conf");
        String sqlStatementDrop = "DROP TABLE IF EXISTS hivetest";
        String sqlStatementCreate = "CREATE TABLE hivetest (message String) STORED AS PARQUET";
        String sqlStatementInsert = "INSERT INTO hivetest VALUES (\"helloworld\")";
        String sqlStatementSelect = "SELECT * from hivetest";
        Statement stmt = hiveConnection.createStatement();
        stmt.execute(sqlStatementDrop);
        stmt.execute(sqlStatementCreate);
        stmt.execute(sqlStatementInsert);
        ResultSet rs = stmt.executeQuery(sqlStatementSelect);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        stmt.execute(sqlStatementDrop);

        // Impala
        Connection impalaConnection = null;
        impalaConnection = DriverManager.getConnection("jdbc:hive2://dn1:21050/;principal=impala/_HOST@" + kerberosRealm);
        sqlStatementDrop = "DROP TABLE IF EXISTS impalatest";
        sqlStatementCreate = "CREATE TABLE impalatest (message String) STORED AS PARQUET";
        sqlStatementInsert = "INSERT INTO impalatest VALUES (\"helloworld\")";
        sqlStatementSelect = "SELECT * from impalatest";
        // Init Statement
        stmt = impalaConnection.createStatement();
        // Invalidate metadata to update changes
        stmt.execute(sqlStatementDrop);
        stmt.execute(sqlStatementCreate);
        stmt.execute(sqlStatementInsert);
        rs = stmt.executeQuery(sqlStatementSelect);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        stmt.execute(sqlStatementDrop);
    }
}
