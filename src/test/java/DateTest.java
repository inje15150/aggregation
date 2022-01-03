import date.WantDateCreate;
import dbconnection.DatabaseCon;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DateTest {

    public static void main(String[] args) {

        DatabaseCon databaseCon = new DatabaseCon();
        databaseCon.connection();

        databaseCon.rules_insert(2,3,3,60);

    }
}
