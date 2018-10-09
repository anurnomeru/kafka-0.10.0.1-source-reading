package kafka.examples;

/**
 * Created by Anur IjuoKaruKas on 2018/9/13
 */
public class ConsumerTest {

    public static void main(String[] args) {
        Consumer consumerThread = new Consumer("KafkaLearning");
        consumerThread.start();
    }
}
