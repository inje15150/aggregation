package date;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class WantDateCreate {

    public synchronized List<String> hourTimeRangeList() {
        List<String> hourList = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");

        for (int i = 0; i <= 24; i++) {
            hourList.add(df.format(cal.getTime()) + ":00:00");
            cal.add(Calendar.HOUR, +1);
        }
        return hourList;
    }

    // 하루 10분 집계 타입명
    public synchronized String tenMinuteTypeName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        DateFormat df = new SimpleDateFormat("MM");

        return df.format(cal.getTime()) + "_ten_minute";
    }

    // 하루 시간 단위 집계 타입명
    public synchronized String hourTypeName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        DateFormat df = new SimpleDateFormat("MM");

        return df.format(cal.getTime()) + "_hour";
    }

    // 하루 시간 단위 집계 타입명
    public synchronized String dayTypeName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        DateFormat df = new SimpleDateFormat("MM");

        return df.format(cal.getTime()) + "_day";
    }


    // 연도 별 인덱스명
    public synchronized String indexName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        DateFormat df = new SimpleDateFormat("yyyy");
        return "agg_data_" + df.format(cal.getTime());
    }

    public synchronized String dayDateFieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("dd");
        return df.format(cal.getTime());
    }

    // 일별 날짜
    public synchronized String currentDateFieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        DateFormat df = new SimpleDateFormat("dd");
        return df.format(cal.getTime());
    }

    public synchronized String tenMinuteAgoFieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -10);
        DateFormat df = new SimpleDateFormat("dd");
        return df.format(cal.getTime());
    }

    // 타입명 추출
    public synchronized String typeName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime());
    }

    // 10min, date 타입명 추출
    public synchronized String lastTypename() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime());
    }

    // 00_00 10분 단위 field
    public synchronized String min_fieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -10);
        DateFormat df = new SimpleDateFormat("HH_mm");
        return df.format(cal.getTime()).substring(0, 4) + "0";
    }

    // 00_00 시간 단위 field
    public synchronized String hour_fieldName(int hour) {
        if (hour < 10) {
            return "0" + hour + "_00";
        } else {
            return hour + "_00";
        }
    }

    // 00-00 일 단위 field
    public synchronized String day_fieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("dd");
        return df.format(cal.getTime());
    }

    // 10분 단위 gte
    public synchronized String min_gte() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -10);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        return df.format(cal.getTime()).substring(0, 15) + "0:00";

    }

    // 10분 단위 lt
    public synchronized String min_lt() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return df.format(cal.getTime()).substring(0, 15) + "0:00";
    }

    // 일 단위 gte
    public synchronized String day_gte() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime()) + " 00:00:00";
    }

    // 일 단위 lt
    public synchronized String day_lt() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime()) + " 00:00:00";
    }

    public synchronized String day_agg_time() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());

        DateFormat df = new SimpleDateFormat("HH");
        return df.format(cal.getTime());
    }
}
