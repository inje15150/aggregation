package search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import thread.DayThread;
import thread.HourThread;
import thread.TenMinThread;

public class Search {

    static final Logger log = LogManager.getLogger(Search.class);

    public static void main(String[] args) {

        TenMinThread tenMinThread = new TenMinThread();
        tenMinThread.setName("10_Min_Thread");
        tenMinThread.start();
        log.info("[{}] Start !!", tenMinThread.getName());

        HourThread hourThread = new HourThread();
        hourThread.setName("Hour_Thread");
        hourThread.start();
        log.info("[{}] Start !!", hourThread.getName());

        DayThread dayThread = new DayThread();
        dayThread.setName("Daily_Thread");
        dayThread.start();
        log.info("[{}] Start !!", dayThread.getName());

    }
}
