package com.ELTTool;

import org.json.JSONObject;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;




/**
 *SQL Server DBConnection<br><br>
 *Connection file format ...<br><br>
 *{<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Type": "SQLServer",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Server": "<i>server</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Database": "<i>database</i>",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Reference": "<i>reference to use on job file</i>"<br>
 *},<br>
 */
public class SQLServer extends DBConnection {


    private static Logger logger = Logger.getLogger(SQLServer.class.getName());


    private static String SQLServerJDBCStart = "jdbc:sqlserver://";
    private static String ClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    public String database;
    public String server;

    private java.sql.Connection conn;

    private transient PreparedStatement prepStmt;


    public SQLServer(JSONObject parms, String jobName, String component) {

        connectionType = ConnectionTypes.Database;
        connectionRef = parms.getString(connKW.Reference.toString());
        dbType = DBTypes.Snowflake;

        this.jobName = jobName;
        this.component = component;
        this.database = parms.getString(connKW.Database.toString());
        this.server = parms.getString(connKW.Server.toString());

    }









    //------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------
    // general methods, almost all are overridden from parent class





    //------------------------------------------------------------------------------------------------------
    @Override
    public ResultSet executeQuery(String Query, boolean suppressLog) throws SQLException {

        if (!suppressLog)
            logger.info(LoggingUtils.getLogEntry("Executing query against SQL Server: " + Query, jobName, jobId, groupId));

        ResultSet rs = null;

        Statement cs = conn.createStatement();
        rs = cs.executeQuery(Query);

        return  rs;

    }





    //------------------------------------------------------------------------------------------------------
    @Override
    public void setPreparedStatement(String stmt) throws SQLException {

        logger.info(LoggingUtils.getLogEntry("Preparing stmt against SQL Server: " + stmt, jobName, jobId, groupId));
        prepStmt = conn.prepareStatement(stmt);

    }














    //------------------------------------------------------------------------------------------------------
    @Override
    public void executeStatement(String SqlStatement, boolean suppressLog) throws SQLException {

        if (!suppressLog)
            logger.info(LoggingUtils.getLogEntry("Executing stmt against SQL Server: " + SqlStatement, jobName, jobId, groupId));

        Statement cs = conn.createStatement();
        cs.execute(SqlStatement);

    }





    // --------------------------------------------------------------------------------
    // these are not used for sql server right now, so are just there so the compiler doesn't complain



    @Override
    public void setPrepParm(String type, int position, Object value) throws SQLException {

    }




    @Override
    public void executePrepStmt() throws SQLException {

        prepStmt.executeQuery();


    }






    //------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------------
    // deal with connections


    @Override
    public void shutdownConnection() throws SQLException {

        logger.info(LoggingUtils.getLogEntry("Closing SQL Server connection : " + connectionRef, jobName, jobId, groupId));

        if (conn != null)
            conn.close();

    }



    @Override
    public void startConnection(int statementTimeout, Long jobId, Long groupId) throws Exception {

        logger.info(LoggingUtils.getLogEntry("Starting SQL Server connection: " + connectionRef, jobName, jobId, groupId));

        this.jobId = jobId;
        this.groupId = groupId;

        String connString = "";

        //logger.info("Setting connection properties");

        // build connection properties
        Properties properties = new Properties();
        properties.put("integratedSecurity", "true");
        properties.put("databaseName", database);
        properties.put("server", server);


        connString = SQLServerJDBCStart + server;
        logger.info(LoggingUtils.getLogEntry("Connection string: " + connString, jobName, jobId, groupId));
        Class.forName(ClassName);
        conn = DriverManager.getConnection(connString, properties);


        // don't do anything with the statemnt timeout at this time JMS 7/1/2020

    }





}
