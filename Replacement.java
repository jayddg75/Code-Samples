package com.ELTTool;



/**
 *Static class and method to help ease replacement in sql statements
 */
public class Replacement {




    public final static String schemaKW = "${schema}";
    public final static String prefixKW = "${prefix}";
    public final static String fileNameKW  = "${fileName}";
    public final static String recCountKW =  "${recCount}";
    public final static String tableNameKW = "${tableName}";
    public final static String tableKW  = "${table}";
    public final static String awsSecretKeyKW = "${awsSecretKey}";
    public final static String awsKeyIdKW = "${awsKeyId}";
    public final static String queryIdKW = "${queryId}";
    public final static String sessionIdKW = "${sessionId}";







    public static String schemaAndPrefix(String schema, String prefix, String sql) {
        return sql.replace(schemaKW, schema).replace(prefixKW, prefix);
    }


    public static String schemaAndTableName(String schema, String tableName, String sql) {
        return sql.replace(schemaKW, schema).replace(tableNameKW, tableName);
    }


    public static String schema(String schema, String sql) {
        return sql.replace(schemaKW, schema);
    }


    public static String awsIdAndKey(String keyId, String secretKey, String sql){
        return sql.replace(awsKeyIdKW, keyId).replace(awsSecretKeyKW, secretKey);
    }


    public static String queryAndSession(String queryId, String sessionId, String sql){
        return sql.replace(queryIdKW, queryId).replace(sessionIdKW, sessionId);
    }


}
