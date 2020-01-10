package com.datagen.Utils;

public class Utils {

    public static  String getPrintableTimeDifference(long startTime, long endTime) {
        long duration = endTime - startTime;
        long seconds = (duration / 1000) % 60;
        long minutes = ((duration / (1000 * 60)) % 60);
        long hours = ((duration / (1000 * 60 * 60)) % 24);
        return  hours + ":" + minutes + ":" + seconds;
    }
}
