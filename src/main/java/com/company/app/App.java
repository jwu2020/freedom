package com.company.app;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;


public class App {

    // Initialise ES client
    private static RestHighLevelClient initialise() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http"),
                        new HttpHost("localhost", 9202, "http")
                ));
        return client;
    }

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = initialise();


        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();
        BulkRequest request = new BulkRequest();

        try {
            // Load array from file, and index each json object
            Object obj = jsonParser.parse(new FileReader("src/main/resources/data.json"));

            JSONArray arr = (JSONArray) obj;
            System.out.println("size: " + arr.size());

            for (int i = 0; i < arr.size(); i++) {
                System.out.println(arr.size());
                JSONObject jsonObject = (JSONObject) arr.get(i);
                System.out.println(jsonObject.toJSONString());
                request.add(new IndexRequest("property").id(Integer.toString(i + 16))
                        .source(jsonObject));
            }


        } catch ( Exception e) {
            e.printStackTrace();
        }

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        // Search for the index
        SearchRequest searchRequest = new SearchRequest("property");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        // Print everything in index
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            System.out.println(hits[i].getSourceAsString());
        }

        client.close();
    }

}
