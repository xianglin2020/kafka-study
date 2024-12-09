package store.xianglin.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 拦截器
 *
 * @author xianglin
 * @see ProducerConfig.INTERCEPTOR_CLASSES_CONFIG
 */
public class CountingProducerInterceptor implements ProducerInterceptor<String, String> {
    private final static Logger log = LoggerFactory.getLogger(CountingProducerInterceptor.class);

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    static AtomicLong numSend = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        numSend.incrementAndGet();
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        numAcked.incrementAndGet();
    }

    @Override
    public void close() {
        executorService.close();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var windowSize = Long.parseLong(Objects.toString(configs.get("counting.interceptor.window.size.ms"), "2000"));
        executorService.scheduleAtFixedRate(CountingProducerInterceptor::run, windowSize, windowSize, TimeUnit.MILLISECONDS);
    }

    public static void run() {
        log.info("numSend = [ {} ]", numSend.getAndSet(0));
        log.info("numAcked = [ {} ]", numAcked.getAndSet(0));
    }
}
