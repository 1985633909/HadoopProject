/**
 * @author 19856
 * @since 2023/4/12-23:07
 */

import org.apache.log4j.Logger;

/*模拟日志产生*/
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args)  {

        int index = 0;
        while (true) {
            PaymentInfo p = new PaymentInfo();
            String message = p.random();
            logger.info(message);
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }
}

