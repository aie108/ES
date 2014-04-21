package com.manitpornsut.search.es.test;

/**
 *
 * @author suparerk
 */
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.node.NodeBuilder.*;

public class TestEasySearch {
    
    private static final String URL = "http://engineer.utcc.ac.th/?p=news";
    
    public static void main(String[] args) throws IOException {
        int i=0;
        Document URLpath = Jsoup.connect(URL).get();
        org.jsoup.select.Elements elements = URLpath.getElementsByTag("section");
        try {
               
            // basic parameters
            String indexName = "Test_simple";
            String documentType = "message";
            String documentId = "1";
            // create client
            Client client = new TransportClient().addTransportAddress(
                    new InetSocketTransportAddress("localhost", 9300));
            // create index
            CreateIndexRequestBuilder createIndexRequestBuilder
                    = client.admin().indices().prepareCreate(indexName);
            createIndexRequestBuilder.execute().actionGet();
            // add documents
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName,
                    documentType, (documentId) + i);

            //import data from url
            for (Element element : elements) {
                for (Element e : element.select("article")) {
                    String title = e.getElementsByTag("strong").get(0).text();
                    String des = e.getElementsByTag("div").text();
                    // build json object    
                    XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
                    contentBuilder.field("title", title);
                    contentBuilder.field("data", des);
                    contentBuilder.endObject();
                    indexRequestBuilder.setSource(contentBuilder);
                    IndexResponse response = indexRequestBuilder.execute().actionGet();
                    System.out.println("Response is: " + response);
                    i++;
                }
            }
            //node.close();
        } catch (IOException ex) {
            Logger.getLogger(TestEasySearch.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
