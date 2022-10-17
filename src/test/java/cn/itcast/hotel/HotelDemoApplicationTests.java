package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;

/**
 * @author shenjie
 * @version 1.0
 */
@SpringBootTest
class HotelDemoApplicationTests {

    @Autowired
    private IHotelService hotelService;

    @Test
    void contextLoads(){
//        Map<String, List<String>> filters = hotelService.getFilters();
//        System.out.println(filters);
    }
}
