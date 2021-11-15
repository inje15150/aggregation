import date.WantDateCreate;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTest {

    public static void main(String[] args) {
        WantDateCreateTest wantDateCreate = new WantDateCreateTest();

        System.out.println(wantDateCreate.min_gte());
        System.out.println(wantDateCreate.min_lt());

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(2021, Calendar.DECEMBER, 14, 0, 0, 0));

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(calendar.getTime()));
        calendar.add(Calendar.MINUTE, -10);

        System.out.println(df.format(calendar.getTime()));

        System.out.println("field : " + wantDateCreate.day_fieldName());
        System.out.println("gte : " + wantDateCreate.day_gte());
        System.out.println("lt : " + wantDateCreate.day_lt());

//        System.out.println("type : " + wantDateCreate.typeName());
        if (wantDateCreate.day_fieldName().equals("13")) {
            System.out.println("type : " + wantDateCreate.lastTypename());
        } else {
            System.out.println("type : " + wantDateCreate.typeName());
        }


        System.out.println(wantDateCreate.day_agg_time());



    }
}
