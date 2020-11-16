package com.ELTTool;


import java.util.logging.Logger;

/**
 * This takes an epoch and converts to an hh:mm:ss format for component and job run time display.
 * No error handling here to problems get pushed up to the calling class to handle
 */
public class TimeConvert {



    /**
     * Converts the epoch to a formatted string
     * @param time the passed in epoch
     * @return the formatted string
     */
    public static String LongToTime(Long time) {

            long s = time / 1000 % 60;
            long m = time / 1000 / 60 % 60;
            long h = time / 1000 / 60 / 60 % 60;

            return String.format("%d:%02d:%02d", h,m,s);


    }


}
