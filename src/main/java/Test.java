import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.*;


/**
 * Created by fpschina on 16-1-26.
 */
public class Test {


    public static void main(String[] args) throws IOException {
//        Settings settings = Settings.settingsBuilder().put("cluster.name", "dip-application").build();
//        TransportClient transportClient = TransportClient.builder().settings(settings).build();
//
//        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.13.56.52"), 9300));
//
//        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
//
//        bulkRequestBuilder.add(transportClient.prepareIndex("test1", "info").setSource(jsonBuilder().startObject()
//                .field("name", "wowowo")
//                .field("age", 12)
//                .field("money", 102.333f)
//                .field("right", 1453945866)
//                .endObject()));
//
//
//        bulkRequestBuilder.get();






    }
}
