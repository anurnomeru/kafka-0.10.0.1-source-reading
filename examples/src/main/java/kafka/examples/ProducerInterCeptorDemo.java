package kafka.examples;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
/**
 * Created by Anur IjuoKaruKas on 2018/6/15
 */
public class ProducerInterCeptorDemo implements ProducerInterceptor {

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return null;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
