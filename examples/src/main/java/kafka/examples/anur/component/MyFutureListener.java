package kafka.examples.anur.component;

/**
 * Created by Anur IjuoKaruKas on 2018/8/25
 *
 * 自己实现一个listener
 */
public interface MyFutureListener<T> {

    void onSuccess(T value);

    void onFailure();
}
