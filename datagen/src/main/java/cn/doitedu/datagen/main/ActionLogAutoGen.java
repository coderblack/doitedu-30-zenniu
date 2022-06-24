package cn.doitedu.datagen.main;

import cn.doitedu.datagen.module.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.util.*;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-27
 * @desc 行为日志生成模拟器（自动连续生成）
 * <p>
 * {
 * "account": "Vz54E9Ya",
 * "appId": "cn.doitedu.app1",
 * "appVersion": "3.4",
 * "carrier": "中国移动",
 * "deviceId": "WEISLD0235S0934OL",
 * "deviceType": "MI-6",
 * "ip": "24.93.136.175",
 * "latitude": 42.09287620431088,
 * "longitude": 79.42106825764643,
 * "netType": "WIFI",
 * "osName": "android",
 * "osVersion": "6.5",
 * "releaseChannel": "豌豆荚",
 * "resolution": "1024*768",
 * "sessionId": "SE18329583458",
 * "timeStamp": 1594534406220
 * "eventId": "productView",
 * "properties": {
 * "pageId": "646",
 * "productId": "157",
 * "refType": "4",
 * "refUrl": "805",
 * "title": "爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码",
 * "url": "https://item.jd.com/36506691363.html",
 * "utm_campain": "4",
 * "utm_loctype": "1",
 * "utm_source": "10"
 * }
 * }
 * <p>
 * <p>
 * kafka中要先创建好topic
 * [root@hdp01 kafka_2.11-2.0.0]# bin/kafka-topics.sh --create --topic yinew_applog --partitions 2 --replication-factor 1 --zookeeper hdp01:2181,hdp02:2181,hdp03:2181
 * <p>
 * 创建完后，检查一下是否创建成功：
 * [root@hdp01 kafka_2.11-2.0.0]# bin/kafka-topics.sh --list --zookeeper hdp01:2181
 */
public class ActionLogAutoGen {
    //public static volatile boolean flag = true;

    public static void main(String[] args) throws Exception {
        // 第一次运行（或者后期需要重新初始化），设置为true
        boolean isInitial = false;
        // 第一次运行（或者后期需要重新初始化），设置为初始用户数，否则为 “增量用户数”
        int needNewUser =  100;
        // 是否需要将增量用户合并到历史用户并保存
        boolean needSave = true;
        // 用户数据保存目录
        String hisUserDataPath = "data/users/";
        // 输出方式：console 或  kafka
        String collectorType = "kafka";
        String topic = "mall-app-log";
        String host = "doitedu:9092";


        File hisUserDir = new File(hisUserDataPath);

        HashMap<String, LogBean> hisUsers = new HashMap<>();
        // 如果是初始运行，而需要的新增用户数为0，则矛盾退出
        if (isInitial && needNewUser < 1) {
            System.out.println("配置参数矛盾，啥也没做");
            return;
        } else if (isInitial) {
            // 否则，清除数据目录
            FileUtils.deleteDirectory(hisUserDir);
            if (!hisUserDir.exists()) FileUtils.forceMkdir(hisUserDir);
        }

        Collection<File> files = FileUtils.listFiles(hisUserDir, new String[]{"dat"}, false);

        if (files.size() > 0) {
            // 2022-06-18_09-46-30.dat
            ArrayList<File> lst = new ArrayList<>(files);
            lst.sort(new Comparator<File>() {
                @Override
                public int compare(File o1, File o2) {
                    return Long.compare(o1.lastModified(), o2.lastModified());
                }
            });

            File hisFile = lst.get(lst.size() - 1);
            // 加载历史用户
            hisUsers = UserUtils.loadHisUsers(hisFile);
        }

        if(needNewUser > 0) {
            System.out.println("准备生成增量新用户");
            // 添加新用户
            UserUtils.addNewUsers(hisUsers, needNewUser, needSave);
        }

        // 转成带状态用户数据
        List<LogBeanWrapper> wrapperedUsers = UserUtils.userToWrapper(hisUsers);

        System.out.println("日活用户总数：" + wrapperedUsers.size() + "-------");

        // 多线程并行生成日志
        genBatchToCollector(collectorType,wrapperedUsers, 3, 5 ,topic ,host);

    }

    private static void genBatchToCollector(String collectorType, List<LogBeanWrapper> wrapperedUsers, int threads, int logPerThrad ,String topic ,String host) {
        int partSize = wrapperedUsers.size() / threads;

        for (int i = 0; i < threads; i++) {
            List<LogBeanWrapper> userPart = new ArrayList<>();

            for (int j = i * partSize; j < (i != threads - 1 ? (i + 1) * partSize : wrapperedUsers.size()); j++) {
                userPart.add(wrapperedUsers.get(j));
            }

            Collector collector = null;
            if(collectorType.equals("kafka")) {
                collector = new CollectorKafkaImpl(topic,host);
            }else{
                collector = new CollectorConsoleImpl();
            }
            new Thread(new LogRunnable(userPart, collector, 10)).start();
        }
    }

}
