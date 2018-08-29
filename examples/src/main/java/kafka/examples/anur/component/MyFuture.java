package kafka.examples.anur.component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Anur IjuoKaruKas on 2018/8/25
 *
 * 主要是自己写一个 adaptor 和 listener
 */
public class MyFuture<T> {

    private T result;

    private List<MyFutureListener> listenerList = new ArrayList<>();

    private boolean succeeded = false;

    private boolean failed = false;

    public boolean isSucceeded() {
        return succeeded;
    }

    public boolean isFailed() {
        return failed;
    }

    public T getResult() {
        return result;
    }

    public void complete(T value) {
        System.out.println("√compete - 调用onSuccess");
        System.out.println(String.format("val 的值为%s，类为%s", value, value.getClass()
                                                                       .getSimpleName()));
        result = value;
        onSuccess();
        succeeded = true;
    }

    public void raise() {
        onFailure();
        failed = true;
    }

    public MyFuture addListener(MyFutureListener myFutureListener) {
        System.out.println("√添加了listener" + myFutureListener.getClass()
                                                            .getSimpleName());
        listenerList.add(myFutureListener);
        return this;
    }

    public <S> MyFuture<S> compose(final MyAdaptor<T, S> myAdaptor) {
        final MyFuture<S> adapted = new MyFuture<>();
        System.out.println("√compose - adaptor 转换中");
        System.out.println("创建了新的 MyFuture 对象");
        System.out.println("在新的 MyFuture 对象上注册了新的 listener");
        addListener(new MyFutureListener<T>() {

            @Override
            public void onSuccess(T value) {
                System.out.println("将调用 adaptor 的 onSuccess 方法");
                myAdaptor.onSucceeded(value, adapted);
            }

            @Override
            public void onFailure() {

            }
        });
        System.out.println("在这个 listener onSuccess 后，会调用传入 myAdaptor 的 onSuccess 方法");
        return adapted;
    }

    private void onSuccess() {
        System.out.println("√onSuccess - 调用listener");
        for (MyFutureListener myFutureListener : listenerList) {
            myFutureListener.onSuccess(this.result);
        }
    }

    private void onFailure() {
        for (MyFutureListener myFutureListener : listenerList) {
            myFutureListener.onFailure();
        }
    }
}
