package schedule;


import aggregation.Aggregation;
import date.WantDateCreate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;

import java.io.IOException;

public class TenMinuteAggregation {

    private final Logger log = LogManager.getLogger(TenMinuteAggregation.class);
    private int retryCount = 0;
    private final static String FINISH = "FINISH";


    public void tenMinuteAggregation() throws IOException {
        try {
            WantDateCreate dateCreate = new WantDateCreate();
            Aggregation aggregation = new Aggregation();

            // TODO: 2021-11-17
            if (!(dateCreate.tenMinuteAgoFieldName().equals(dateCreate.currentDateFieldName()))) {
                aggregation.aggregation(dateCreate.indexName(), dateCreate.min_fieldName(), dateCreate.tenMinuteTypeName(), dateCreate.min_gte(), dateCreate.min_lt(), dateCreate.tenMinuteAgoFieldName(), dateCreate.lastTypename());
            } else {
                aggregation.aggregation(dateCreate.indexName(), dateCreate.min_fieldName(), dateCreate.tenMinuteTypeName(), dateCreate.min_gte(), dateCreate.min_lt(), dateCreate.currentDateFieldName(), dateCreate.typeName());
            }
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
