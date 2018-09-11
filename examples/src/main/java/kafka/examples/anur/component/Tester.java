package kafka.examples.anur.component;

/**
 * Created by Anur IjuoKaruKas on 2018/8/25
 */
public class Tester extends Thread {

    private MyFuture myFuture;

    public Tester(MyFuture myFuture) {
        this.myFuture = myFuture;
    }

    public static void main(String[] args) throws InterruptedException {
        MyFuture<String> myFuture = new MyFuture<>();

        MyFuture<Book> bookMyFuture = myFuture.compose(new BookAdaptor());

        // 不应该在 bookMyFuture 生效
        myFuture.addListener(new MyFutureListener<String>() {

            @Override
            public void onSuccess(String value) {
                System.out.println("监听器成功");
            }

            @Override
            public void onFailure() {
                System.out.println("监听器失败");
            }
        });

        Tester tester = new Tester(myFuture);
        tester.start();
        while (!myFuture.isSucceeded() && !myFuture.isFailed()) {
            Thread.sleep(100);
        }

        //        Tester tester1 = new Tester(bookMyFuture);
        //        tester1.start();
        //        while (!bookMyFuture.isSucceeded() && !bookMyFuture.isFailed()) {
        //            Thread.sleep(100);
        //        }
        //        System.out.println(Json.encode(bookMyFuture.getResult()));
    }

    @Override
    public void run() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        myFuture.complete("啦啦啦啦啦");
        //        myFuture.raise();
        super.run();
    }
}
