package elasticsearch_api;

import com.google.gson.Gson;
import entity.Cpu;
import entity.Memory;
import entity.Total;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.significant.ParsedSignificantTerms;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AggregationApi {

    public final static String[] ES_HOST = {"192.168.60.80", "192.168.60.81", "192.168.60.82"};
    public final static String[] FIELD_NAME = {"node1", "node2", "node3"};
    private final static String AGENT_INFO_INDEX_NAME = "agent_info";
    public final static int PORT = 9200;
    static final Logger log = LogManager.getLogger(AggregationApi.class);
    private final Gson gson = new Gson();
    private final static String REQUEST = "REQUEST";
    private final static String SETTINGS = "SETTINGS";
    private final static String OK = "OK";
    private final static String ERROR = "ERROR";
    private final static String NOT_FOUND = "NOT_FOUND";
    private final static String INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR";

    // 10분 단위 cpu sum, avg, max, min, count
    public Map<String, Total> aggregation(RestHighLevelClient client, String gte, String lt, String agentInfoTypeName, String fieldName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(AGENT_INFO_INDEX_NAME);
        searchRequest.types(agentInfoTypeName);

        // 집계 쿼리 보낼 검색소스 빌더 생성
        SearchSourceBuilder search = new SearchSourceBuilder();

        // 범위 별 조회
        SearchSourceBuilder searchSourceBuilder = search
                .size(0)
                .query(QueryBuilders
                        .boolQuery()
                        .filter(QueryBuilders
                                .rangeQuery("event_time")
                                .gte(gte)
                                .lt(lt)
                        )
                ); // 10분, 1시간, 1일 별 gte, lt 값 부여
        log.info("[{}] Set the range to aggregation. [{}] ~ [{}]", SETTINGS, gte, lt);
        // 집계 쿼리 메서드 호출
        SearchSourceBuilder query = addAggQuery(searchSourceBuilder);

        searchRequest.source(query);// query 를 source(body)에 넣기

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT); // 실제 search 쿼리 전달
        log.info("[{}] Aggregation query forwarding.", REQUEST);

        int status = response.status().getStatus();
        if (status == HttpStatus.SC_OK) {
            log.info("[{}] [{}/{}] : agent_info aggregation success !!", OK, agentInfoTypeName, fieldName);
        } else if (status == HttpStatus.SC_NOT_FOUND) {
            log.error("[{}] [{}/{}] [{}]/Status:[{}] error", ERROR,agentInfoTypeName, fieldName, NOT_FOUND, status);
            throw new ElasticsearchStatusException("Elasticsearch response status error.", RestStatus.NOT_FOUND);
        } else if (status == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
            log.error("[{}] [{}/{}] [{}]/Status:[{}] error", ERROR, agentInfoTypeName, fieldName, INTERNAL_SERVER_ERROR, status);
            throw new ElasticsearchStatusException("Elasticsearch response status error.", RestStatus.NOT_FOUND);
        }

        Aggregations aggregations = response.getAggregations(); // 응답 값 Aggregations 객체 타입으로 반환

        return aggregationResponse(aggregations, gson);
    }

    // 각 agent 별 반복 쿼리 메서드
    private SearchSourceBuilder addAggQuery(SearchSourceBuilder query) {
   // 3개 노드 반복 쿼리
        // TODO: 2021-11-17
        query.aggregation(AggregationBuilders
                .significantTerms("cpu_mem_agg")
                .field("ip.keyword")
                .subAggregation(AggregationBuilders
                        .stats("cpu")
                        .field("cpu"))
                .subAggregation(AggregationBuilders
                        .stats("memory")
                        .field("memory"))
        );

        log.info("Determining which data to aggregation");
        return query;
    }

    // aggregation 응답 값 파싱 후 CpuAggregation, MemoryAggregation 객체에 담기
    private Map<String, Total> aggregationResponse(Aggregations aggregations, Gson gson) {
        Map<String, Aggregation> asMap = aggregations.getAsMap();
        Map<String, Total> nodeNameMap = new ConcurrentHashMap<>(); // {"node 이름" : "total 데이터"} 형태로 담을 map 생성

        ParsedSignificantTerms cpu_mem_agg = (ParsedSignificantTerms) asMap.get("cpu_mem_agg");
        cpu_mem_agg.getBuckets().iterator().forEachRemaining(
                bucket -> {
                    Cpu cpu = new Cpu(); // 집계 된 CPU 데이터 담을 객체 생성
                    Memory memory = new Memory(); // 집계 된 Memory 데이터 담을 객체 생성

                    String key = (String) bucket.getKey();
                    ParsedStats cpuStats = (ParsedStats) bucket.getAggregations().getAsMap().get("cpu");
                    ParsedStats memoryStats = (ParsedStats) bucket.getAggregations().getAsMap().get("memory");

                    String cpuToJson = gson.toJson(cpuStats);
                    cpu = gson.fromJson(cpuToJson, Cpu.class);
                    cpu.rounds();

                    log.info("[{}] [{}] Parsing cpu aggregate data", OK, key);

                    String memoryToJson = gson.toJson(memoryStats);
                    memory = gson.fromJson(memoryToJson, Memory.class);
                    memory.rounds();

                    log.info("[{}] [{}] Parsing memory aggregate data", OK, key);

                    // Total 객체 생성자 시점에 데이터 넣기
                    Total total = new Total(cpu, memory, nowDate());

                    nodeNameMap.put(key, total);
                    log.info("[{}] data parsing success.", OK);
                }
        );
        return nodeNameMap;
    }

    public String nowDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(calendar.getTime());
    }
}
