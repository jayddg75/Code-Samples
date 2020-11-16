package com.ELTTool;

import javax.swing.*;
import java.io.File;
import java.util.logging.Logger;

/**
 *This brings up the file chooser dialog used in various places
 */
public class FileChooser {


    private static Logger logger = Logger.getLogger(FileChooser.class.getName());

    JFileChooser jf;

    public File chooseFile (String jobName, Long jobId, Long groupId)  {

            JFileChooser jf = new JFileChooser();
            int returnVal = jf.showOpenDialog(jf);

            // if a file was selected or not
            if (!(returnVal == JFileChooser.APPROVE_OPTION)) {
                return null;
            } else {
                return jf.getSelectedFile();
            }


    }



}
