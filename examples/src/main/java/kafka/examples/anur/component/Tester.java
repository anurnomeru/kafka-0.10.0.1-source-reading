package kafka.examples.anur.component;

/**
 * Created by Anur IjuoKaruKas on 2018/8/25
 */
public class Tester extends Thread {

    private MyFuture<String> myFuture;

    public Tester(MyFuture<String> myFuture) {
        this.myFuture = myFuture;
    }

    public static void main(String[] args) throws InterruptedException {
        MyFuture<String> myFuture = new MyFuture<>();

        myFuture.addListener(new MyFutureListener() {

            @Override
            public void onSuccess() {
                System.out.println("监听器成功");
            }

            @Override
            public void onFailure() {
                System.out.println("监听器失败");
            }
        });
        Tester tester = new Tester(myFuture);
        tester.start();

        System.out.println("喵喵喵");
        while (!myFuture.isSucceeded() && !myFuture.isFailed()) {
            Thread.sleep(100);
        }
        System.out.println(myFuture.getResult());
    }

    @Override
    public void run() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        myFuture.complete("啦啦啦啦啦");
        myFuture.raise();
        super.run();
    }
}
