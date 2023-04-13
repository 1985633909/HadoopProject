import org.junit.Test;

/**
 * @author 19856
 * @since 2023/4/12-23:18
 */
public class Test01 {

    /**
     * 测试是否可以成功生成订单
     */
    @Test
    public void testAddProduce(){
        PaymentInfo paymentInfo = new PaymentInfo();
        String s = paymentInfo.random();
        System.out.println("s = " + s);
    }
}
