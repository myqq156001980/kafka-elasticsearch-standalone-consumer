package org.elasticsearch.kafka.indexer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fpschina on 16/2/16.
 */
public class CurrentDate {
    public static String getTodayDate(){
        String f = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(f);
        Date date = new Date();

        return simpleDateFormat.format(date);
    }


}
