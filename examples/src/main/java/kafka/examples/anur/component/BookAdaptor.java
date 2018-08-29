package kafka.examples.anur.component;

/**
 * Created by Anur IjuoKaruKas on 2018/8/25
 */
public class BookAdaptor implements MyAdaptor<String, Book> {

    private MyFuture<Book> bookMyFuture;

    @Override
    public void onSucceeded(String value, MyFuture<Book> future) {
        future.raise();
        System.out.println("???");
    }

    @Override
    public void onFailure() {

    }
}
