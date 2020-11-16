package com.ELTTool;




import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;



/**
 *The holds main and does pivotal work on loading paths, config files, and dlls
 */
public class Main {


    private static Logger logger;
    public static Front m;

    public static final String fpLoadFolderKW = "loadFolder";
    public static final String fpJobFolderKW =  "jobFolder";
    public static final String fpOpFolderKW = "opFolder";
    public static final String fpSqlFolderKW = "sqlFolder";
    public static final String fpGroupFolderKW = "groupFolder";
    public static final String fpIconsFolderKW = "iconsFolder";




    public static void main(String[] args) {


        try {





            String userDir = System.getProperty("user.dir");
            System.out.println("user director: " + userDir);


            //----------------------------------------------------------------------------------
            // make sure the root folders exist


            String opFolderName = "Op";
            String loadFolderName = "Loading";
            String jobFolderName = "Jobs";
            String sqlFolderName = "SQLFiles";
            String groupFolderName = "Groups";
            String logFolderName = "Logs";
            String iconsFolderName = "Icons";


            File opFolder = new File(userDir + "\\" + opFolderName);
            File loadFolder = new File(userDir + "\\" + loadFolderName);
            File jobFolder = new File(userDir + "\\" + jobFolderName);
            File groupFolder = new File(userDir + "\\" + groupFolderName);
            File sqlFolder = new File(opFolder.toString() + "\\" + sqlFolderName);
            File logFolder = new File(opFolder.toString() + "\\" + logFolderName);
            File iconsFolder = new File(opFolder.toString() + "\\" + iconsFolderName);



            if(!opFolder.exists())
                opFolder.mkdir();

            if(!loadFolder.exists())
                loadFolder.mkdir();

            if (!jobFolder.exists())
                jobFolder.mkdir();

            if (!groupFolder.exists())
                groupFolder.mkdir();

            if (!logFolder.exists())
                logFolder.mkdir();


            if (!sqlFolder.exists())
                sqlFolder.mkdir();



            Properties fileProps = new Properties();
            fileProps.put(fpLoadFolderKW, loadFolder.toString());
            fileProps.put(fpJobFolderKW, jobFolder.toString());
            fileProps.put(fpOpFolderKW, opFolder.toString());
            fileProps.put(fpSqlFolderKW, fileProps.getProperty(fpOpFolderKW) + "\\SQLFiles");
            fileProps.put(fpGroupFolderKW, groupFolder.toString());
            fileProps.put(fpIconsFolderKW, iconsFolder.toString());


            //---------------------------------------------------------------------------------------------
            // check for the config and auth files and if there are none then move the ones from the Jar out ....


            String configFileName = "config.properties";
            String loggingFileName = "logging.properties";
            String authFileName = "mssql-jdbc_auth-8.2.1.x64.dll";


            File configFile = new File(opFolder.toString() + "\\" + configFileName);
            File loggingFile = new File(opFolder.toString() + "\\" + loggingFileName);
            File authFile = new File(opFolder.toString() + "\\" + authFileName);


            if (!configFile.exists())
                throw new Exception("No config file exists: " + configFile.toString() );

            if (!loggingFile.exists())
                throw new Exception("No log properties file exists: " + loggingFile.toString() );

            if (!authFile.exists())
                throw new Exception("No SQL Server auth file exists: " + authFile.toString() );





            // ---------------------------------------------------------------------------------------------
            // set the config and logging file


            System.setProperty("java.util.logging.config.file", opFolder.toString() + "\\logging.properties");
            logger = Logger.getLogger(Main.class.getName());
            logger.info(System.getProperty("java.util.logging.config.file"));


            Properties prop = new Properties();
            FileInputStream ip = new FileInputStream(configFile.toString());
            prop.load(ip);

            logger.info("Loaded config file");




            // ---------------------------------------------------------------------------------------------
            // Add the auth ddl for sql server, what a pain in the ass this is.  This is how is solved it; link below
            // Can't just set "java.library.path".  I have to make sure the source path is in the usr path
            // then it won't throw the unsatisfied link error. :(
            // http://fahdshariff.blogspot.com/2011/08/changing-java-library-path-at-runtime.html


            Field usrPathsField = ClassLoader.class.getDeclaredField("usr_paths");
            usrPathsField.setAccessible(true);
            String[] paths = (String[])usrPathsField.get(null);

            // check to see if the path is already there
            boolean addedFlag = false;
            for(String path : paths)
                if(path.equals(opFolder.toString()))
                    addedFlag = true;


            //System.out.println(addedFlag);
            //for(String s : paths )
                //System.out.println(s);


            // if not, add it in
            if (!addedFlag) {

                String[] newPaths = Arrays.copyOf(paths, paths.length + 1);
                newPaths[newPaths.length-1] = opFolder.toString();
                usrPathsField.set(null, newPaths);

            }

            // now load the dll
            System.loadLibrary("mssql-jdbc_auth-8.2.1.x64");



            // ---------------------------------------------------------------------------------------------
            // start up the front end

            m = new Front(prop, fileProps);
            m.start();




        } catch (Exception e) {
            e.printStackTrace();
            NotificationBox.displayNotification(e.getMessage());
            System.exit(-1);
        }


    }


}
