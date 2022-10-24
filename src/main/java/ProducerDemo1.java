import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @projectName: Kafka
 * @package: PACKAGE_NAME
 * @className: ProducerDemo
 * @author: Cypress_Xiao
 * @description: kafka定时生产消费案例案例
 * @date: 2022/8/19 20:05
 * @version: 1.0
 */

public class ProducerDemo1 {
    public static void main(String[] args) {
        //1.创建配置文件对象
        Properties props = new Properties();
        //设置kafka集群连接
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop001:9092");
        //设置key和value的序列化方式
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //3.创建topic

        for (int i = 0; i < 10000; i++) {
            //4.创建要传递的数据的封装对象
            ProducerRecord<String, String> records = new ProducerRecord<String, String>("doit33","kafka"+i,i+"");
            //5.传递数据
            producer.send(records);
        }
        //关闭
        producer.close();
    }
}
