package test;

import com.alibaba.dubbo.performance.demo.agent.AgentApp;

public class AgentAppConsumer {
    // agent会作为sidecar，部署在每一个Provider和Consumer机器上
    // 在Provider端启动agent时，添加JVM参数-Dtype=provider -Dserver.port=30000 -Ddubbo.protocol.port=20889
    // 在Consumer端启动agent时，添加JVM参数-Dtype=consumer -Dserver.port=20000
    // 添加日志保存目录: -Dlogs.dir=/path/to/your/logs/dir。请安装自己的环境来设置日志目录。

    public static void main(String[] args) throws Exception {
        setSystemPropertiesIfNull("type", "consumer");
        setSystemPropertiesIfNull("run_mode_local", "true");
        setSystemPropertiesIfNull("server.host", "127.0.0.1");
        setSystemPropertiesIfNull("server.port", "20000");
        setSystemPropertiesIfNull("threads", "2");
        setSystemPropertiesIfNull("logs.dir", "logs");
        setSystemPropertiesIfNull("etcd.url", "http://127.0.0.1:2379");
        AgentApp.main(args);
    }
    
    public static void setSystemPropertiesIfNull(String key, String value) {
        String pro = System.getProperty(key);
        if (pro == null || pro.length() == 0) {
            System.setProperty(key, value);
        }
    }
    
    
}
