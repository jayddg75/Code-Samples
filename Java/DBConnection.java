package com.ELTTool;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *This is the database connection super class.  DBConnections are per job and componentId; no shared connections across anything.<br>
 *<br>
 *In a job file, for a parameter, a dbConnection JSON Object is as follows ...<br>
 *<br>
 *<ul>
 *<li>type - Required:String - <i>Snowflake</i>, <i>SQLServer</i>, etc ...</li>
 *<li>statementTimeout - Required:Integer - the number of seconds before killing a running statement</li>
 *<li>context and values - Required:String - the reference name for the type listed in the connection file</li>
 *</ul>
 *<br>
 *Example ...<br>
 *<br>
 *"dbConnection":{<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"Snowflake",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"statementTimeout": 180,<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"dev": "devAdmin",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"qa": "qaRO",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"stage", "stageRO",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"prod", "prodRO"<br>
 *}<br>
 */
public abstract class DBConnection extends Connection {



    public DBTypes dbType;
    public ArrayList<String> spParmTypes;
    public String component;


    public static final String dbConnStmtTimeoutKW = "statementTimeout";
    public static final String dbConnTypeKW = "type";






    public abstract ResultSet executeQuery(String Query, boolean suppressLog) throws SQLException;
    public abstract void setPreparedStatement(String stmt) throws SQLException;
    public abstract void executeStatement(String SqlStatement, boolean suppressLog) throws SQLException;
    public abstract void setPrepParm(String type, int position, Object value) throws SQLException;
    public abstract void executePrepStmt() throws SQLException;




}
