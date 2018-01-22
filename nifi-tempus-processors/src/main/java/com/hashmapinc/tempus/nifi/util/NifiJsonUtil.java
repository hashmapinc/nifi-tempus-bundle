package com.hashmapinc.tempus.nifi.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Mitesh Rathore
 * @project nifi-tempus-processors
 */
public class NifiJsonUtil {

    public static final String TS_FIELDNAME = "ts";
    public static final String DS_FIELDNAME = "ds";

    public static final String KEY_CONSTRUCT_DELIMETER = ",";

    public static final String LOG_CHILD_ELEMENT = "values";


    public static long changeTimeToLong(String logTime){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        long longTime = 0;
        Date date = null;
        try {
            date = formatter.parse(logTime);
            longTime = date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return longTime;

    }


}
