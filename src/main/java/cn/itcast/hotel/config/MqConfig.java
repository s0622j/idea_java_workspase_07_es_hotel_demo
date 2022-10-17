package cn.itcast.hotel.config;

import cn.itcast.hotel.constants.HotelMqConstants;
import cn.itcast.hotel.pojo.Hotel;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author shenjie
 * @version 1.0
 */

@Configuration
public class MqConfig {
    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(HotelMqConstants.EXCHANGE_NAME,true,false);
    }

    @Bean
    public Queue insertQuere(){
        return new Queue(HotelMqConstants.INSERT_QUEUE_NAME,true);
    }

    @Bean
    public Queue deleteQueue(){
        return new Queue(HotelMqConstants.DELETE_QUEUE_NAME,true);
    }

    @Bean
    public Binding inertQueueBinding(){
        return BindingBuilder.bind(insertQuere()).to(topicExchange()).with(HotelMqConstants.INSERT_KEY);
    }

    @Bean
    public Binding deleteQueueBinding(){
        return BindingBuilder.bind(deleteQueue()).to(topicExchange()).with(HotelMqConstants.DELETE_KEY);
    }
}
