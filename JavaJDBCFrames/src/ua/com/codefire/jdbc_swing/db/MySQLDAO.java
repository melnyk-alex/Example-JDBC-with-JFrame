/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ua.com.codefire.jdbc_swing.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

/**
 * Data Access Object (DAO)
 *
 * @author human
 */
public class MySQLDAO {

    private Properties properties;

    public MySQLDAO(String hostname, String username, String password) {
        this.properties = new Properties();
        this.properties.put("hostname", hostname);
        this.properties.put("username", username);
        this.properties.put("password", password);
    }

    public MySQLDAO(Properties properties) {
        this.properties = properties;
    }

    public Connection getConnection() throws SQLException {
        String connectionString = String.format("jdbc:mysql://%s/?useSSL=false",
                properties.getProperty("hostname"));

        return DriverManager.getConnection(connectionString,
                properties.getProperty("username"), properties.getProperty("password"));
    }

    public List<String> getDatabaseList() throws SQLException {
        List<String> databaseList = new ArrayList<>();

        try (Connection conn = getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery("SHOW DATABASES");

            while (rs.next()) {
                databaseList.add(rs.getString(1)); // where clause
            }
        }

        return databaseList;
    }

    public List<String> getTablesList(String database) throws SQLException {
        List<String> tablesList = new ArrayList<>();

        try (Connection conn = getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery("SHOW TABLES FROM " + database);

            while (rs.next()) {
                tablesList.add(rs.getString(1)); // where clause
            }
        }

        return tablesList;
    }

    public TableModel getTableDataModel(String database, String table) throws SQLException {
        try (Connection conn = getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(String.format("SELECT * FROM `%s`.`%s`", database, table));

            ResultSetMetaData metaData = rs.getMetaData();

            Vector<String> columns = new Vector<>();

            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i + 1);
                columns.add(columnName);
            }

            Vector<Vector<Object>> data = new Vector<>();

            while (rs.next()) {
                Vector<Object> row = new Vector<>();

                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    row.add(rs.getObject(i + 1));
                }

                data.add(row);
            }

            return new DefaultTableModel(data, columns);
        }
    }

    public String dumpDatabase(String database) throws SQLException {
        StringBuilder databaseDumpBuilder = new StringBuilder();

        try (Connection conn = getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery("SHOW TABLES FROM " + database);

            while (rs.next()) {
                String tableName = rs.getString(1);

                String tableDump = dumpTable(conn, database, tableName);

                databaseDumpBuilder.append(tableDump);
            }
        }

        return databaseDumpBuilder.toString();
    }

    private String dumpTable(Connection conn, String database, String table) throws SQLException {
        StringBuilder tableDumpBuilder = new StringBuilder();

        tableDumpBuilder.append(String.format("-- CREATE TABLE %s\n", table));
        tableDumpBuilder.append("DROP TABLE IF EXISTS ").append(table).append(";\n");
        tableDumpBuilder.append("CREATE TABLE ").append(table).append(" (\n");

        try (Statement stmt = conn.createStatement();) {
            ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM `%s`.`%s`", database, table));

            ResultSetMetaData tableMetaData = rs.getMetaData();

            for (int i = 1; i <= tableMetaData.getColumnCount(); i++) {
                String columnName = tableMetaData.getColumnName(i);
                String columnTypeName = tableMetaData.getColumnTypeName(i);
                int precision = tableMetaData.getPrecision(i);

                tableDumpBuilder.append("\t").append(columnName).append(" ").append(columnTypeName)
                        .append("(").append(precision).append(")");

                if (i != tableMetaData.getColumnCount()) {
                    tableDumpBuilder.append(",\n");
                }
            }

            tableDumpBuilder.append("\n);\n\n");
            tableDumpBuilder.append(String.format("-- INSERT QUERIES %s\n", table));

            // QUERIES
            tableDumpBuilder.append(String.format("INSERT INTO `%s` VALUES\n", table));

            while (rs.next()) {
                tableDumpBuilder.append("(");
                for (int i = 1; i <= tableMetaData.getColumnCount(); i++) {
                    tableDumpBuilder.append("'").append(rs.getObject(i)).append("'");

                    if (i != tableMetaData.getColumnCount()) {
                        tableDumpBuilder.append(",");
                    }
                }
                tableDumpBuilder.append(")");
                
                if (!rs.isLast()) {
                    tableDumpBuilder.append(",");
                }
            }
            tableDumpBuilder.append(";\n\n");

        }

        return tableDumpBuilder.toString();
    }

}
