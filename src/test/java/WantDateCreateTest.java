import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class WantDateCreateTest {

    public synchronized List<String> hourTimeRangeList() {
        List<String> hourList = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 20, 0, 7, 10));
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");

        for (int i = 0; i <= 24; i++) {
            hourList.add(df.format(cal.getTime()) + ":00:00");
            cal.add(Calendar.HOUR, +1);
        }

        return hourList;
    }

    // 타입명 추출
    public synchronized String typeName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5, 10));
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime());
    }

    public synchronized String lastTypename() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5, 0));
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime());
    }

    // 00_00 10분 단위 field
    public synchronized String min_fieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5, 0));
        cal.add(Calendar.MINUTE, -10);
        DateFormat df = new SimpleDateFormat("HH_mm");
        return df.format(cal.getTime()).substring(0, 4) + "0";
    }

    // 00_00 시간 단위 field
    public synchronized String hour_fieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5, 0));
        cal.add(Calendar.HOUR, -1);
        DateFormat df = new SimpleDateFormat("HH");
        return df.format(cal.getTime()) + "_00";
    }

    // 00-00 일 단위 field
    public synchronized String day_fieldName() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5, 0));
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("dd");
        return df.format(cal.getTime());
    }

    // 10분 단위 gte
    public synchronized String min_gte() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5,0));
        cal.add(Calendar.MINUTE, -10);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        return df.format(cal.getTime()).substring(0, 15) + "0:00";

    }

    // 10분 단위 lt
    public synchronized String min_lt() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5,0));
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return df.format(cal.getTime()).substring(0, 15) + "0:00";
    }

    // 시간 단위 gte
    public synchronized String hour_gte() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5,0));
        cal.add(Calendar.HOUR, -1);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");

        return df.format(cal.getTime()) + ":00:00";
    }

    // 시간 단위 lt
    public synchronized String hour_lt() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5,0));
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");

        return df.format(cal.getTime()) + ":00:00";
    }

    // 일 단위 gte
    public synchronized String day_gte() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5, 0));
        cal.add(Calendar.DATE, -1);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(cal.getTime()) + " 00:00:00";
    }

    // 일 단위 lt
    public synchronized String day_lt() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 5,0));

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
