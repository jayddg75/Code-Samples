package com.ELTTool;

import net.snowflake.client.jdbc.SnowflakeConnection;
import net.snowflake.client.jdbc.SnowflakePreparedStatement;
import net.snowflake.client.jdbc.SnowflakeStatement;
import org.json.JSONObject;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;




/**
 *Snowflake DBConnection<br><br>
 *Connection file format ...<br><br>
 *{<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Type": "Snowflake",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Account": "<i>host.region.snowflakecomputing.com</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"User": "<i>userName</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Password": "<i>password</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Warehouse": "<i>warehouse</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Database": "<i>database</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Schema": "<i>schema</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Reference": "<i>reference to use on job files</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Role": "<i>role if needed</i>"<br>
 *}<br>
 */
public class Snowflake extends DBConnection {


    private static Logger logger = Logger.getLogger(Snowflake.class.getName());

    private static String SnowflakeDriverClass = "net.snowflake.client.jdbc.SnowflakeDriver";
    private static String SnowflakeJDBCStart = "jdbc:snowflake://";


    public String account;
    public String userName;
    public String password;
    public String warehouse;
    public String database;
    public String schema;
    public String role;

    private transient PreparedStatement prepStmt;
    private Statement stmt;

    private java.sql.Connection conn;
    private String sessionId;
    private String queryId;

    private final String spBoolean = "boolean";
    private final String spFloat = "float";
    private final String spVarchar = "varchar";






    public Snowflake(JSONObject parms, String jobName, String component) {


        connectionType = ConnectionTypes.Database;
        connectionRef = parms.getString(connKW.Reference.toString());
        dbType = DBTypes.SQLServer;

        this.jobName = jobName;
        this.component = component;

        account = parms.getString(connKW.Account.toString());
        userName = parms.getString(connKW.User.toString());
        password = parms.getString(connKW.Password.toString());
        warehouse = parms.getString(connKW.Warehouse.toString());
        database = parms.getString(connKW.Database.toString());
        schema = parms.getString(connKW.Schema.toString());


        if (parms.has(connKW.Role.toString())){
            this.role = parms.getString(connKW.Role.toString());
        } else {
            this.role = null;
        }


        // set the supported SP parm types at this time
        spParmTypes = new ArrayList<>();
        spParmTypes.add(spBoolean);
        spParmTypes.add(spFloat);
        spParmTypes.add(spVarchar);




    }





    public String getSessionId() { return sessionId; }
    public String getQueryId() { return queryId; }



    //---------------------------------------------------------------------------------------------------------
    //---------------------------------------------------------------------------------------------------------
    //---------------------------------------------------------------------------------------------------------
    // general methods





    //------------------------------------------------------------------------------------------------------
    @Override
    public ResultSet executeQuery(String Query, boolean suppressLog) throws SQLException {

        try {

            if(!suppressLog)
                logger.info(LoggingUtils.getLogEntry("Executing query against Snowflake: " + Query, jobName, jobId, groupId));

            stmt = conn.createStatement();
            queryId = stmt.unwrap(SnowflakeStatement.class).getQueryID();
            return stmt.executeQuery(Query);


        } catch (SQLException s) {
            logSFError(s, stmt.unwrap(SnowflakeStatement.class).getQueryID(), Query);
            throw s;
        }


    }








    //------------------------------------------------------------------------------------------------------
    @Override
    public void setPreparedStatement(String stmt) throws SQLException {

        try {

            prepStmt = conn.prepareStatement(stmt);

        } catch (SQLException s) {
            logSFError(s, prepStmt.unwrap(SnowflakePreparedStatement.class).getQueryID(), stmt);
            throw s;
        }

    }







    @Override
    public void executePrepStmt() throws SQLException {

        try {

            logger.info(LoggingUtils.getLogEntry("Executing prepared statement: " + prepStmt.unwrap(SnowflakePreparedStatement.class).toString(), jobName, jobId, groupId));
            prepStmt.execute();

        } catch (SQLException s) {
            logSFError(s, prepStmt.unwrap(SnowflakePreparedStatement.class).getQueryID());
            throw s;
        }



    }



    //------------------------------------------------------------------------------------------------------
    @Override
    public void executeStatement(String SqlStatement, boolean suppressLog) throws SQLException {


        try {

            if (!suppressLog)
                logger.info(LoggingUtils.getLogEntry("Executing stmt against Snowflake: " + SqlStatement, jobName, jobId, groupId));

            stmt = conn.createStatement();
            stmt.execute(SqlStatement);

            queryId = stmt.unwrap(SnowflakeStatement.class).getQueryID();

        } catch (SQLException s) {
            logSFError(s, stmt.unwrap(SnowflakeStatement.class).getQueryID(), SqlStatement);
            throw s;
        }



    }







    @Override
    public void setPrepParm(String type, int position, Object value) throws SQLException {

        try {

            switch (type.toLowerCase()) {

                case spBoolean:
                    prepStmt.setBoolean(position, (boolean) value);
                    break;
                case spFloat:
                    prepStmt.setFloat(position, (float) value);
                    break;
                case spVarchar:
                    prepStmt.setString(position, String.valueOf(value));
                    break;


            }


        } catch (SQLException s) {
            logSFError(s, prepStmt.unwrap(SnowflakePreparedStatement.class).getQueryID());
            throw s;
        }





    }







    //---------------------------------------------------------------------------------------------------------
    //---------------------------------------------------------------------------------------------------------
    //---------------------------------------------------------------------------------------------------------
    // deal with connections





    //------------------------------------------------------------------------------------------------
    @Override
    public void shutdownConnection() throws SQLException {

        try {

            logger.info(LoggingUtils.getLogEntry("Closing Snowflake connection: " + connectionRef, jobName, jobId, groupId));

            if (conn != null)
                if(!conn.isClosed())
                    conn.close();



        } catch (SQLException s) {
            logSFError(s);
            throw s;
        }


    }





    @Override
    public void startConnection(int statementTimeout, Long jobId, Long groupId) throws SQLException, ClassNotFoundException {


        try {


            logger.info(LoggingUtils.getLogEntry("Starting Snowflake connection: " + connectionRef, jobName, jobId, groupId));

            this.jobId = jobId;
            this.groupId = groupId;

            String connString = "";

            Properties properties = new Properties();
            properties.put("user", userName);
            properties.put("password", password);
            properties.put("warehouse", warehouse);
            properties.put("db", database);
            properties.put("schema", schema);
            properties.put("statement_timeout_in_seconds", statementTimeout);

            if (role != null) {
                properties.put("role", role);
            }

            connString = SnowflakeJDBCStart + account;
            Class.forName(SnowflakeDriverClass);

            logger.info(LoggingUtils.getLogEntry("Connection string: " + connString, jobName, jobId, groupId));

            conn = DriverManager.getConnection(connString, properties);


            sessionId = conn.unwrap(SnowflakeConnection.class).getSessionID();

            // always pass the session Id's to the logs, no matter what setting
            logger.severe(LoggingUtils.getLogEntry("Snowflake sessionId: " + sessionId, jobName, jobId, groupId));



        } catch (SQLException s) {
            logSFError(s);
            throw s;

        }


    }







    //---------------------------------------------------------------------------------------------------------
    //---------------------------------------------------------------------------------------------------------
    //---------------------------------------------------------------------------------------------------------
    // log error stuff



    private void logSFError(SQLException s, String queryId) {

        logger.severe(LoggingUtils.getLogEntry("Snowflake QueryId: " + queryId, jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake SessionId: " + sessionId, jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake Error Code: " + s.getErrorCode(), jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake Error Message: " + s.getMessage(), jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake SQL State: " + s.getSQLState(), jobName, jobId, groupId));

    }


    private void logSFError(SQLException s) {

        logger.severe(LoggingUtils.getLogEntry("Snowflake SessionId: " + sessionId, jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake Error Code: " + s.getErrorCode(), jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake Error Message: " + s.getMessage(), jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake SQL State: " + s.getSQLState(), jobName, jobId, groupId));

    }



    private void logSFError(SQLException s, String queryId, String sqlStmt) {

        logger.severe(LoggingUtils.getLogEntry("Snowflake QueryId: " + queryId, jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake SessionId: " + sessionId, jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake Error Code: " + s.getErrorCode(), jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake Error Message: " + s.getMessage(), jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake SQL State: " + s.getSQLState(), jobName, jobId, groupId));
        logger.severe(LoggingUtils.getLogEntry("Snowflake SQL Statement: " + sqlStmt, jobName, jobId, groupId));


    }





}
