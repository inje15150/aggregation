package thread;


import aggregation.Aggregation;
import date.WantDateCreate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;

import java.io.IOException;

public class TenMinThread extends Thread {

    private final Logger log = LogManager.getLogger(TenMinThread.class);
    private int retryCount = 0;

    @Override
    public void run() {
        while (true) {
            try {
                long start = System.currentTimeMillis();
                WantDateCreate dateCreate = new WantDateCreate();
                Aggregation aggregation = new Aggregation();

                if (dateCreate.min_fieldName().equals("23_50")) {
                    aggregation.aggregation("ten_minute", dateCreate.min_fieldName(), dateCreate.lastTypename(), dateCreate.min_gte(), dateCreate.min_lt());
                } else {
                    aggregation.aggregation("ten_minute", dateCreate.min_fieldName(), dateCreate.typeName(), dateCreate.min_gte(), dateCreate.min_lt());
                }
                retryCount = 0;

                long end = System.currentTimeMillis();
                long diffTime = end - start;
//                noinspection BusyWait
                Thread.sleep(600000 - diffTime);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
                break;
            } catch (IOException | ElasticsearchStatusException e) {
                log.error(e.getMessage());

                // IOException, ElasticsearchStatusException 발생 시 5회 retry
                if (retryCount < 5) {
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
