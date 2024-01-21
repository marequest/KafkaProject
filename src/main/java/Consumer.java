import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

    private final static Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer Start");

        String groupId = "my-application";

    }
}
