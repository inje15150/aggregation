package elasticsearch_api;

import com.google.gson.Gson;
import entity.Cpu;
import entity.Memory;
import entity.Total;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
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
    public Map<String, Total> aggregation(RestHighLevelClient client, String gte, String lt, String typeName, String fieldName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(AGENT_INFO_INDEX_NAME);
        searchRequest.types(typeName);

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
                        )); // 10분, 1시간, 1일 별 gte, lt 값 부여
        log.info("[{}] Set the range to aggregation. [{}] ~ [{}]", SETTINGS, gte, lt);
        // 집계 쿼리 메서드 호출
        SearchSourceBuilder query = addAggQuery(searchSourceBuilder);

        searchRequest.source(query); // query 를 source(body)에 넣기
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT); // 실제 search 쿼리 전달
        log.info("[{}] Aggregation query forwarding.", REQUEST);

        int status = response.status().getStatus();
        if (status == 201) {
            log.info("[{}] [{}/{}] : agent_info aggregation success !!", OK,typeName, fieldName );
        } else if (status == 404) {
            log.error("[{}] [{}/{}] [{}]/Status:[{}] error", ERROR,typeName, fieldName, NOT_FOUND, status);
        } else if (status == 500) {
            log.error("[{}] [{}/{}] [{}]/Status:[{}] error", ERROR, typeName, fieldName, INTERNAL_SERVER_ERROR, status);
        }

        Aggregations aggregations = response.getAggregations(); // 응답 값 Aggregations 객체 타입으로 반환

        return aggregationResponse(aggregations, gson, fieldName);
    }

    // 각 agent 별 반복 쿼리 메서드
    private SearchSourceBuilder addAggQuery(SearchSourceBuilder query) {
        for (String host : ES_HOST) { // 3개 노드 반복 쿼리
            String fieldName = null;
            if (host.equals("192.168.60.80")) {
                fieldName = FIELD_NAME[0];
            } else if (host.equals("192.168.60.81")) {
                fieldName = FIELD_NAME[1];
            } else {
                fieldName = FIELD_NAME[2];
            }
            query.aggregation(AggregationBuilders
                    .filter(fieldName, QueryBuilders
                            .termQuery("ip", host))
                    .subAggregation(AggregationBuilders
                            .stats("cpu")
                            .field("cpu"))
                    .subAggregation(AggregationBuilders
                            .stats("memory")
                            .field("memory")));
        }
        log.info("Determining which data to aggregation");
        return query;
    }

    // aggregation 응답 값 파싱 후 CpuAggregation, MemoryAggregation 객체에 담기
    private Map<String, Total> aggregationResponse(Aggregations aggregations, Gson gson, String fieldName) {
        List<Aggregation> aggregationList = aggregations.asList(); // aggregation list
        Map<String, Total> nodeNameMap = new ConcurrentHashMap<>(); // {"node 이름" : "total 데이터"} 형태로 담을 map 생성
        Cpu cpu = new Cpu(); // 집계 된 CPU 데이터 담을 객체 생성
        Memory memory = new Memory(); // 집계 된 Memory 데이터 담을 객체 생성

        // 각 노드 별 데이터 파싱 하기 위한 반복
        for (Aggregation agg : aggregationList) {
            ParsedFilter parsedFilter = (ParsedFilter) agg; // "fieldName" : "Map"
            ParsedStats cpuStats = (ParsedStats) parsedFilter.getAggregations().getAsMap().get("cpu"); // "cpu" : "stats"
            ParsedStats memoryStats = (ParsedStats) parsedFilter.getAggregations().getAsMap().get("memory"); // "memory" : "stats"

            // ParsedFilter 변수 gson 활용하여 Cpu, Memory class 형태로 파싱
            String cpuToJson = gson.toJson(cpuStats);
            cpu = gson.fromJson(cpuToJson, Cpu.class);
            log.info("[{}] [{}] Parsing cpu aggregate data", OK, agg.getName());

            String memoryToJson = gson.toJson(memoryStats);
            memory = gson.fromJson(memoryToJson, Memory.class);
            log.info("[{}] [{}] Parsing memory aggregate data", OK, agg.getName());

            // Total 객체 생성자 시점에 데이터 넣기
            Total total = new Total(cpu, memory, nowDate());

            nodeNameMap.put(agg.getName(), total);
            log.info("[{}] data parsing success.", OK);
        }
        return nodeNameMap;
    }

    public String nowDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(calendar.getTime());
    }
}
