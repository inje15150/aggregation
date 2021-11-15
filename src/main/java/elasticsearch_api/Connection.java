package elasticsearch_api;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Connection {

    private final RestHighLevelClient client;
    private final static Logger log = LogManager.getLogger(Connection.class);

    public Connection() {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(AggregationApi.ES_HOST[0], AggregationApi.PORT, "http")));
        log.info("elasticsearch connection success !!");
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
