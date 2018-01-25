package com.wxmimperio.hbase;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SimpleClient {
    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(4); //固定为4的线程队列
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 6, 1, TimeUnit.DAYS, queue);
        for (int i = 0; i < 10; i++) {
            executor.execute(new Thread(new ThreadPoolTest(), "TestThread".concat("" + i)));
            int threadSize = queue.size();
            //System.out.println("线程队列大小为-->" + threadSize);
            if (threadSize == 4) {
                boolean flag = queue.offer(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("我是新线程，看看能不能搭个车加进去！");
                    }
                });
                while (!flag) {
                    flag = queue.offer(new Runnable() {
                        @Override
                        public void run() {
                            System.out.println("加不进去");
                        }
                    });
                    if (flag) {
                        System.out.println("加进去了！");
                    }
                }
            }
        }
        executor.shutdown();
    }
}
