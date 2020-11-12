package myflink;

import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Transaction;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * @ClassName TransactionSupplier
 * @Description TODO
 * @Author 中森明菜
 * @Date 2020/11/12 10:53
 * @Version 1.0
 */
public class TransactionSupplier implements Supplier<Transaction> {

    private static final Random generator = new Random();

    private final Iterator<Long> accounts =
            Stream.generate(() -> Stream.of(1L, 2L, 3L, 4L, 5L))
                    .flatMap(UnaryOperator.identity())
                    .iterator();

    private static final Iterator<LocalDateTime> timestamps =
            Stream.iterate(
                    LocalDateTime.of(2000, 1, 1, 1, 0),
                    time -> time.plusMinutes(5).plusSeconds(generator.nextInt(58) + 1))
                    .iterator();

    public static void main(String[] args) {
        LocalDateTime next = timestamps.next();
        System.out.println(next);
    }

    @Override
    public Transaction get() {
        return null;
    }
}
