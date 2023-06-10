package org.mdao07.demos.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerProperties {

    private Properties properties;
    private Logger logger;

    ProducerProperties(Logger logger) {
        this.properties = new Properties();
        this.logger = logger;
    }

    public void setCommonProperties() {
        // Security configs
        setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        setProperty(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"3f27zjaSlpULADekW5YWxt\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzZjI3emphU2xwVUxBRGVrVzVZV3h0Iiwib3JnYW5pemF0aW9uSWQiOjczNjMyLCJ1c2VySWQiOjg1NjIwLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyZDJkZWQ4YS1lOGM0LTRhMjUtYjBjNS03YzhlODlmZjE4NjcifX0.P4DxRlFnr9J9UQGzogk7w59NIECIAZGjnXkzDlotE5Q\";");
        setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");

        // set producer properties
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        // localhost
        //setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public void setProperty(String key, String value) {
        logger.info("Property {} set to {}", key, value);
        properties.setProperty(key, value);
    }

    public Properties getProperties() {
        return properties;
    }
}
