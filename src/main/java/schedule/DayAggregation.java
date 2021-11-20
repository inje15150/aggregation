package schedule;


import aggregation.Aggregation;
import date.WantDateCreate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;

import java.io.IOException;

public class DayAggregation {

    private final Logger log = LogManager.getLogger(DayAggregation.class);
    private int retryCount = 0;
    private final static String FINISH = "FINISH";

    public void dayAggregation() throws IOException {
        try {
            WantDateCreate dateCreate = new WantDateCreate();
            Aggregation aggregation = new Aggregation();

            aggregation.aggregation(dateCreate.indexName(), dateCreate.day_fieldName(), dateCreate.dayTypeName(), dateCreate.day_gte(), dateCreate.day_lt(), dateCreate.dayDateFieldName(), dateCreate.lastTypename());

            retryCount = 0;

        } catch (IOException | ElasticsearchStatusException e) {
            log.error(e.getMessage());

// IOException, ElasticsearchStatusException 발생 시 5회 retry
            if (retryCount < 6) {
                ++retryCount;
                log.info("[{}].. retry.. [{}]", e.getMessage(), retryCount);
            } else {
                log.info("[{}] [{}]failed retry attempts. Terminate the Thread..", FINISH, retryCount);
                throw new IOException(retryCount - 1 + "failed retry attempts.");
            }
        }
    }
}
