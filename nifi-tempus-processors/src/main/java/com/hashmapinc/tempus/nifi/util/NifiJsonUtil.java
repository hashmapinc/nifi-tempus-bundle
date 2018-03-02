package com.hashmapinc.tempus.nifi.util;

import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
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

    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX";




    /**
     *
     * @param logTime
     * @return
     */
    public static long changeTimeToLong(String logTime){
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        long longTime = 0;
        Date date = null;
        try {
            date = formatter.parse(logTime);
            longTime = date.getTime();
        } catch (ParseException e) {
            try {longTime=Long.parseLong(logTime);} catch (Exception ex) {}
            e.printStackTrace();
        }
        return longTime;

    }

    /**
     * Format Long time to date
     * @param longTime
     * @return
     */
    public static String changeLongToDateTime(String longTime){
        String formattedDate = null;
        if(!StringUtils.isEmpty(longTime)){
            try{
                Calendar cal = java.util.Calendar.getInstance();
                cal.setTimeInMillis(Long.valueOf(longTime));
                SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
                formattedDate = dateFormat.format(cal.getTime());
            }catch(Exception exp){
                exp.printStackTrace();
                return longTime;
            }
       }
        return formattedDate;

    }


}
