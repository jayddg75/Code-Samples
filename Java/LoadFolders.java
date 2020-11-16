package com.ELTTool;

import java.io.File;
import java.util.Properties;
import java.util.logging.Logger;



/**
 *The is a helper class that is used by various components to ensure proper folder structure exists
 */
public class LoadFolders {



    private static Logger logger = Logger.getLogger(LoadFolders.class.getName());




    public static void checkLoadFolders(Properties fileProps, String prefix) {


        String topFolder = fileProps.getProperty(Main.fpLoadFolderKW) + "\\" + prefix;
        String loadFolder = topFolder + "\\" + JobManager.componentLoadFolderNameKW;
        String workFolder = topFolder + "\\" + JobManager.componentWorkFolderNameKW;

        File topFile = new File(topFolder);
        File loadFile = new File(loadFolder);
        File workFile = new File(workFolder);

        if (!topFile.exists()) {
            logger.info("Created folder: " + topFile.getName());
            topFile.mkdir();
        }

        if (!loadFile.exists()) {
            logger.info("Created folder: " + loadFile.getName());
            loadFile.mkdir();
        }

        if (!workFile.exists()) {
            logger.info("Created folder: " + workFile.getName());
            workFile.mkdir();
        }



    }



}
