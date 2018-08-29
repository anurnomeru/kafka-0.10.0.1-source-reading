package kafka.examples.anur.component;

/**
 * Created by Anur IjuoKaruKas on 2018/8/25
 *
 * 还有问题
 */
public interface MyAdaptor<F, T> {

    void onSucceeded(F value, MyFuture<T> future);

    void onFailure();
}
