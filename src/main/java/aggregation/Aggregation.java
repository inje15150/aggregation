package aggregation;

import elasticsearch_api.*;
import entity.Total;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Aggregation {

    static final Logger log = LogManager.getLogger(Aggregation.class);
    private final Connection connection = new Connection();
    private final static String OK = "OK";
    private final static String KEY_GET_INDEX_NAME = "agent_info";

    public synchronized void aggregation(String index_name, String fieldName, String typeName, String gte, String lt, String dateFieldName, String agentInfoTypeName) throws IOException, ElasticsearchStatusException {

        // 집계 관련 객체 생성
        AggregationApi aggregationApi = new AggregationApi();

        // {ip : Total} 리턴 값 전달 받음
        // 각 agent 10분 데이터 집계
        Map<String, Total> totals = aggregationApi.aggregation(connection.getClient(), gte, lt, agentInfoTypeName, fieldName);

        // 인덱스 생성 객체
        IndexCreateApi indexCreateApi = new IndexCreateApi(connection.getClient());

        // 인덱스 존재하지 않을 시 인덱스 생성
        if (!indexCreateApi.existIndex(index_name)) {
            log.info("index does not exist. So we start creating the index.");
            indexCreateApi.createIndex(index_name, typeName, fieldName);
            log.info("[{}], index creation complete.", OK);
        }

        // 도큐먼트 존재 여부 및 도큐먼트 id 얻어오기 위한 객체 생성
        DocumentIdSearch search = new DocumentIdSearch();

        // <nodeName, documentId> List
        List<Map<String, String>> docs = search.getDocumentId(connection.getClient(), index_name, typeName);

        // TODO: 2021-11-21 (DB 연동 후 변경 필요)

        Map<String, String> nodeIpMap = getNodes(new ConcurrentHashMap<>()); // {"ip" : "nodeName"}
        Map<String, Object> map = new ConcurrentHashMap<>(); // 최종 데이터 담을 map 생성

        List<String> nodeIp = getIpList(new ArrayList<>());

        // 해당 날짜 필드가 존재하는지 여부
        boolean existDateField = search.isFieldContain(connection.getClient(), index_name, typeName, dateFieldName);

        // 월 변경 시, 도큐먼트가 존재하지 않을 때
        if (docs.isEmpty() || !existDateField) {

            log.info("The document does not exist. insert a document.");
            for (String ip : nodeIp) {

                Total total = totals.get(ip);

                Map<String, Total> totalMap = new ConcurrentHashMap<>();
                totalMap.put(fieldName, total); // fieldName - "00_00" 헝태

                String key = search.getKey(connection.getClient(), KEY_GET_INDEX_NAME, agentInfoTypeName, ip);
                map.put("id", key); // {"id" : "식별이름"}
                map.put("node_name", nodeIpMap.get(ip));
                map.put(dateFieldName, totalMap); // {"11-17" : {"00_00" : "Total 데이터"}}

                InsertDocumentApi insertDocumentApi = new InsertDocumentApi();

                insertDocumentApi.insert(connection.getClient(), map, index_name, typeName, nodeIpMap.get(ip), fieldName, key, dateFieldName);
            }
        }
        // 도큐먼트가 존재 할 때
        // TODO: 2021-11-17(DB 연동 후 변경 필요)
        else {
            log.info("[{}] The document already exists. Update the field.", OK);
            for (String ip : nodeIp) {

                Total total = totals.get(ip); // nodeNam 별 data 필드 값 추출

                Map<String, Total> totalMap = new ConcurrentHashMap<>();
                totalMap.put(fieldName, total);

                String key = search.getKey(connection.getClient(), KEY_GET_INDEX_NAME, agentInfoTypeName, ip);
                map.put("id", key);
                map.put("node_name", nodeIpMap.get(ip));
                map.put(dateFieldName, totalMap);

                UpdateDocumentApi updateDocumentApi = new UpdateDocumentApi();
                // 업데이트 시 document id 필요
                docs.iterator().forEachRemaining(
                    docMap ->
                    {
                        if (docMap.containsKey(nodeIpMap.get(ip))) { // node 별 도큐먼트 id 포함하면 업데이트
                            try {
                                updateDocumentApi.update(index_name, typeName, fieldName, docMap.get(nodeIpMap.get(ip)), map, connection.getClient(), nodeIpMap.get(ip), key, dateFieldName);
                            } catch (IOException e) {
                                log.error(e.getMessage());
                            }
                        }
                    }
                );
            }
        }
    }

    public Map<String, String> getNodes(Map<String, String> nodeIpMap) {
        nodeIpMap.put("192.168.60.80", "node1");
        nodeIpMap.put("192.168.60.81", "node2");
        nodeIpMap.put("192.168.60.82", "node3");

        return nodeIpMap;
    }

    public List<String> getIpList(List<String> nodeIp) {
        nodeIp.add("192.168.60.80");
        nodeIp.add("192.168.60.81");
        nodeIp.add("192.168.60.82");

        return nodeIp;
    }

    // 현재 시간 22일 00:08
    // dateFieldName = 21 (23:50)

    // 현재 시간 22일 00:18
    // dateFieldName = 22 ( 00:00)


    public boolean hasDateChanged(String dateFieldName) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.MINUTE, -10);
        DateFormat df = new SimpleDateFormat("dd");

        return dateFieldName.equals(df.format(cal.getTime()));
    }
}