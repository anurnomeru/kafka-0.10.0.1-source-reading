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
        result = value;
        onSuccess();
        succeeded = true;
    }

    public void raise() {
        onFailure();
        failed = true;
    }

    public MyFuture addListener(MyFutureListener myFutureListener) {
        listenerList.add(myFutureListener);
        return this;
    }

    private void onSuccess() {
        for (MyFutureListener myFutureListener : listenerList) {
            myFutureListener.onSuccess();
        }
    }

    private void onFailure() {
        for (MyFutureListener myFutureListener : listenerList) {
            myFutureListener.onFailure();
        }
    }
}
