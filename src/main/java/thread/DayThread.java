package thread;


import aggregation.Aggregation;
import date.WantDateCreate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;

import java.io.IOException;

public class DayThread extends Thread {

    private final Logger log = LogManager.getLogger(DayThread.class);
    private int retryCount = 0;

    @Override
    public void run() {
        while (true) {
            try {
                WantDateCreate dateCreate = new WantDateCreate();
                Aggregation aggregation = new Aggregation();

                if (dateCreate.day_agg_time().equals("01")) {
                    aggregation.aggregation("day", dateCreate.day_fieldName(), dateCreate.lastTypename(), dateCreate.day_gte(), dateCreate.day_lt());
                } else {
                    //noinspection BusyWait
                    Thread.sleep(3600000);
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage());
                break;
            }  catch (IOException | ElasticsearchStatusException e) {
                log.error(e.getMessage());

                // IOException, ElasticsearchStatusException 발생 시 5회 retry
                if (retryCount < 6) {
                    ++retryCount;
                    log.info("IOException error.. retry.. [{}]", retryCount);
                } else {
                    log.info("[{}] failed retry attempts", retryCount);
                    break;
                }
            }
        }
    }
}