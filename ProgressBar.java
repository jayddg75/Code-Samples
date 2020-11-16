package com.ELTTool;

import javax.swing.*;

public class ProgressBar {


    private JProgressBar pb;
    private JFrame pbFrame;


    public ProgressBar(int startValue, int endValue, String title, boolean displayText) {

        pbFrame = new JFrame();
        pbFrame.setTitle(title);
        pb = new JProgressBar(startValue, endValue);
        pb.setValue(startValue);
        pbFrame.getContentPane().add(pb);

        if (displayText)
            pb.setStringPainted(true);

        pbFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        pbFrame.setSize(600,100);
        pbFrame.setLocationRelativeTo(null);
        pbFrame.setVisible(true);

    }


    public void updateProgressBar(int curValue) {
        pb.setValue(curValue);
    }


    public void updateProgressBar(int curValue, String text) {
        pb.setValue(curValue);
        pb.setString(text);
    }


    public void closeProgressBar() {
        pbFrame.dispose();
    }


}
