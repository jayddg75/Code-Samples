package com.ELTTool;


/**
 * This the top level abstract class for all connections and defines the conectionType and connectionRef that is used across all different connection types.<br>
 * This also holds the enums used in varying places.
 */
abstract public class Connection {


    public ConnectionTypes connectionType;
    public String connectionRef;
    public String jobName;
    public Long jobId;
    public Long groupId;


    public enum ConnectionTypes {
        Database,
        S3,
        Mongo
    }

    public enum DBTypes {
        Snowflake,
        SQLServer
    }


    public enum connKW {
        Reference,
        Type,
        Database,
        Server,
        DBType,
        Account,
        User,
        Password,
        Warehouse,
        Schema,
        Role,
        KeyId,
        SecretKey,
        URI
    }


    public ConnectionTypes getConnectionType() { return connectionType; }
    public String getConnectionRef() { return connectionRef; }


    public abstract void startConnection(int statementTimeout, Long jobId, Long groupId) throws Exception;
    public abstract void shutdownConnection() throws Exception;


}
