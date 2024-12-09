package store.xianglin.kafka.producer;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.Iterator;

public class CustomerGenerator implements Iterable<Customer> {
    @Override
    public Iterator<Customer> iterator() {
        return new Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Customer next() {
                return new Customer(index++, RandomStringUtils.randomAlphanumeric(10));
            }
        };
    }
}
