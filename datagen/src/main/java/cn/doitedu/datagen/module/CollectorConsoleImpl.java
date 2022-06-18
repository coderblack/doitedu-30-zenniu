package cn.doitedu.datagen.module;

public class CollectorConsoleImpl implements Collector {
    @Override
    public void collect(String logdata) {
        System.out.println(logdata);
    }
}
