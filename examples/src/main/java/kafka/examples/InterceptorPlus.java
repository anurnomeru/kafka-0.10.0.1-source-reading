package kafka.examples;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by Anur IjuoKaruKas on 2018/9/5
 */
public class InterceptorPlus implements ProducerInterceptor {

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        Integer val = Integer.valueOf(record.value()
                                            .toString());

        String result = String.valueOf(val + 1);

        return new ProducerRecord(record.topic(),
            record.key(),
            result);
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
