package search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import schedule.DayAggregation;
import schedule.HourAggregation;

import java.io.IOException;
import java.util.*;

public class Search {

    static final Logger log = LogManager.getLogger(Search.class);


    public static void main(String[] args) {

//        Timer minTimer = new Timer();
//        // 10분 스케줄
//        TimerTask minTask = new TimerTask() {
//            @Override
//            public void run() {
//                TenMinuteAggregation tenMinuteAggregation = new TenMinuteAggregation();
//                try {
//                    tenMinuteAggregation.tenMinuteAggregation();
//                } catch (IOException e) {
//                    log.error("Terminate the Thread..");
//                    minTimer.cancel();
//                }
//            }
//        };
//        minTimer.scheduleAtFixedRate(minTask, 0, 600000);


        // 시간 별 스케줄
        Timer hourTimer = new Timer();
        Calendar hourDate = scheduleDate(5);

        TimerTask hourTask = new TimerTask() {
            @Override
            public void run() {
                HourAggregation hourAggregation = new HourAggregation();
                try {
                    hourAggregation.hourAggregation();
                } catch (IOException e) {
                    log.error("Terminate the Thread..");
                    hourTimer.cancel();
                }
            }
        };
        hourTimer.scheduleAtFixedRate(hourTask, hourDate.getTime(), 1000 * 60 * 60 * 24);


        // 일별 스케줄
        Timer dayTimer = new Timer();
        Calendar dayDate = scheduleDate(7);

        TimerTask dayTask = new TimerTask() {
            @Override
            public void run() {
                DayAggregation dayAggregation = new DayAggregation();
                try {
                    dayAggregation.dayAggregation();
                } catch (IOException e) {
                    log.error("Terminate the Thread..");
                    dayTimer.cancel();
                }
            }
        };
        dayTimer.scheduleAtFixedRate(dayTask, dayDate.getTime(), 1000 * 60 * 60 * 24);
    }

    public static Calendar scheduleDate(int minute) {
        Calendar nowDate = Calendar.getInstance();
        nowDate.add(Calendar.DATE, +1);
        Calendar date = Calendar.getInstance();
        date.set(Calendar.DATE, nowDate.getTime().getDate());
        date.set(Calendar.HOUR_OF_DAY, 0);
        date.set(Calendar.MINUTE, minute);
        date.set(Calendar.SECOND, 30);

        return date;
    }

}
