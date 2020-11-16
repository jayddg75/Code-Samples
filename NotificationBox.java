package com.ELTTool;

import javax.swing.*;
import java.util.logging.Logger;


/**
 *Used to display notification by various classes
 */
public class NotificationBox {

    private static Logger logger = Logger.getLogger(NotificationBox.class.getName());



    // use this one if you want to hold up the current thread
    public static void displayNotification(String message) {
        JOptionPane.showMessageDialog(new JFrame(), message);
    }



    // use this one to make sure you don't stop the current thread
    public static void displayRunningNotification(String message) {


        JFrame jf = new JFrame();
        JTextArea ja = new JTextArea(message);
        ja.setEditable(false);
        JPanel jp = new JPanel();
        jp.add(ja);
        JScrollPane jsp = new JScrollPane(jp);
        jf.getContentPane().add(jsp);
        jf.setSize(400, 150);
        jf.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        jf.setVisible(true);


    }





}
