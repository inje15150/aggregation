import date.WantDateCreate;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateTest {

    public static void main(String[] args) {
        WantDateCreateTest wantDateCreate = new WantDateCreateTest();

        List<String> hourTimeRangeList = wantDateCreate.hourTimeRangeList();

        for (int i = 0; i < hourTimeRangeList.size() - 1; i++) {
            System.out.println(hourTimeRangeList.size());
            System.out.println(i + 1 + " hour");
            System.out.println("gte: " + hourTimeRangeList.get(i));
            System.out.println("lt: " + hourTimeRangeList.get(i + 1));
            System.out.println();
        }
    }
}
