package schedule;

import aggregation.Aggregation;
import date.WantDateCreate;
import elasticsearch_api.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;

import java.io.IOException;
import java.util.List;

public class HourAggregation {

    private final Logger log = LogManager.getLogger(HourAggregation.class);
    private int retryCount = 0;
    private final static String FINISH = "FINISH";
    private Connection connection;

    public HourAggregation(Connection connection) {
        this.connection = connection;
    }

    public void hourAggregation() throws IOException, InterruptedException {
        try {
            WantDateCreate dateCreate = new WantDateCreate();

            List<String> hourTimeRangeList = dateCreate.hourTimeRangeList();

            for (int i = 0; i < hourTimeRangeList.size() - 1; i++) {
                String gte = hourTimeRangeList.get(i);
                String lt = hourTimeRangeList.get(i + 1);
                Aggregation aggregation = new Aggregation();

                aggregation.aggregation(dateCreate.indexName(), dateCreate.hour_fieldName(i), dateCreate.hourTypeName(), gte, lt, dateCreate.dayDateFieldName(), dateCreate.lastTypename());

                //noinspection BusyWait
                Thread.sleep(1200);
            }

            retryCount = 0;

        } catch (IOException | ElasticsearchStatusException | IllegalArgumentException e) {
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
