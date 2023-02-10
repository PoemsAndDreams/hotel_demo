package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelDocumentTest {
    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @Test
    void testAddDocument() throws IOException {

//        查询
        Hotel hotel = hotelService.getById(61083L);
//        经纬度转换
        HotelDoc hotelDoc = new HotelDoc(hotel);
//        1.创建Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
//        2.准备请求参数，DSL语句
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
//        3.发送请求
        client.index(request,RequestOptions.DEFAULT);

    }

    @Test
    void testGetDocumentById() throws IOException {
//        1.准备Request对象
        GetRequest request = new GetRequest("hotel","61083");
//        2.发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

        System.out.println(hotelDoc);


    }


    @Test
    void testUpdateDocumentById() throws IOException {
//        1.准备Request对象
        UpdateRequest request = new UpdateRequest("hotel","61083");

        request.doc(
                "name","上海滴水湖皇冠假日酒店-test",
                "address","自由贸易试验区临港新片区南岛1号-test"
        );

        client.update(request,RequestOptions.DEFAULT);

    }


    @Test
    void testDeleteDocumentById() throws IOException {
//        1.准备Request对象
        DeleteRequest request = new DeleteRequest("hotel","61083");

        client.delete(request,RequestOptions.DEFAULT);

    }

    @Test
    void testBulkRequest() throws IOException {

        //        1.准备Request对象
        BulkRequest request = new BulkRequest();

        //        查询
        List<Hotel> hotels = hotelService.list();

//        经纬度转换
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId()
                            .toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        client.bulk(request,RequestOptions.DEFAULT);

    }






    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.188.201:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
