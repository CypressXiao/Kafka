import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @projectName: Kafka
 * @package: PACKAGE_NAME
 * @className: ConsumerDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/19 20:17
 * @version: 1.0
 */

public class ConsumerDemo1 {
    public static void main(String[] args) throws Exception {
        //1.创建配置文件对象
        Properties pro = new Properties();
        pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop001:9092");
        pro.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        pro.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"10000");
        pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group01");
        //2.创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(pro);

        consumer.subscribe(Collections.singletonList("doit33"));
        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE));
            for (ConsumerRecord<String, String> record : poll) {
                String key = record.key();
                String value = record.value();
                System.out.println("key:"+key+",value:"+value);
            }
        }
    }
}
