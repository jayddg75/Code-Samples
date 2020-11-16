package com.ELTTool;

import javax.swing.*;
import java.util.ArrayList;


/**
 *This gets user input in various ways used in several places throughout
 */
public class GetInput {


    public static String GetChoice(String title, String phrase, String[] choices) {

        String selection = (String) JOptionPane.showInputDialog(new JFrame(), title , phrase, JOptionPane.QUESTION_MESSAGE, null, choices, null);
        return selection;

        // null means no choice

    }


    public static String GetChoice(String title, String phrase, ArrayList<String> choices) {

        String[] outList = new String[choices.size()];

        for (int x = 0; x < choices.size(); x++) {
            outList[x] = (String) choices.get(x);
        }

        String selection = (String) JOptionPane.showInputDialog(new JFrame(), title , phrase, JOptionPane.QUESTION_MESSAGE, null, outList, null);
        return selection;

        // null means no choice

    }



    public static int GetYesNoAlter(String title, String question, String YesButtonText, String NoButtonText) {

        String[] buttonText = new String[2];
        buttonText[0] = YesButtonText;
        buttonText[1] = NoButtonText;

        int r =  JOptionPane.showOptionDialog(new JFrame(), question, title, JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE, null, buttonText, null );
        return r;

        // -1 = cancelled, 0 = yes, 1 = no

    }


    public static int GetYesNo(String title, String question) {

        int r =  JOptionPane.showOptionDialog(new JFrame(), question, title, JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE, null, null, null );
        return r;

        // -1 = cancelled, 0 = yes, 1 = false

    }






}
