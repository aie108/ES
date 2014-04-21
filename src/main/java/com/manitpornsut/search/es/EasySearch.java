package com.manitpornsut.search.es;

import com.manitpornsut.search.es.test.TestEasySearch;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.jsoup.nodes.Element;

/**
 * ES - Sample application of ElasticSearch.
 * @author Suparerk Manitpornsut
 * @version 0.1
 *
 */
public class EasySearch {
    protected Client                    client;
    protected CreateIndexRequestBuilder createIndexRequestBuilder;
    protected IndexRequestBuilder       indexRequestBuilder;
    protected IndexResponse             response;
    
    // default settings
    protected String host = "localhost";
    protected int    port = 9300;
    
    public boolean createIndexName(String indexName) {
        return createIndex(host, port, indexName);
    }
    
    public boolean createIndexName(String host, int port, String indexName) {
        boolean status = false;
        
        try {
            // creates client
            client = new TransportClient().addTransportAddress(
                    new InetSocketTransportAddress(host, port));
            
            // prepares index
            createIndexRequestBuilder
                    = client.admin().indices().prepareCreate(indexName);
            
            // execute an action to create the specified indexName at host:port
            createIndexRequestBuilder.execute().actionGet();
            status = true;
        } catch (ElasticsearchException ex) {
            Logger.getLogger(EasySearch.class.getName()).log(Level.SEVERE, null, ex);
        }

        return status;
    }
    
    public IndexRequestBuilder createIndexRequestBuilder(
            String indexName, 
            String docType,
            String docID) {
        IndexRequestBuilder indexRequestBuilder = null;
        try {
            // init indexRequestBuilder
            indexRequestBuilder = client.prepareIndex(indexName,
                docType, docID);
            
        } catch(ElasticsearchException ex) {
            Logger.getLogger(EasySearch.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return indexRequestBuilder;
    }
    
    public IndexResponse executeIndexRequest(
            String indexName, 
            String docType,
            int    docID;
            XContentBuilder contentBuilder) {
        IndexResponse response;
    
        try {
            indexRequestBuilder.setSource(contentBuilder);
            response = indexRequestBuilder.execute().actionGet();
        } catch(ElasticsearchException ex) {
            
        }
    }
    
    public boolean insertDocument(String indexName) {
        boolean status = false;
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
        return status;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
    
    
    
    public static void main( String[] args ) {
        System.out.println( "Hello World!" );
    }
}
