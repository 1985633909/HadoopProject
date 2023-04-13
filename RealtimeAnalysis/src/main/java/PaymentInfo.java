import com.alibaba.fastjson.JSONObject;

import java.util.Random;
import java.util.UUID;

/**
 * @author 19856
 * @since 2023/4/12-23:09
 */
public class PaymentInfo {
    private static final long serialVersionUID =1L;
    private String orderId;//订单编号
    private String productId;//商品编号
    private long productPrice;//商品价格

    public PaymentInfo() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public long getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(long productPrice) {
        this.productPrice = productPrice;
    }

    @Override
    public String toString() {
        return "PaymentInfo{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", productPrice=" + productPrice +
                '}';
    }
    //随机模拟订单数据
    public String random(){
        //创建随即对象
        Random r = new Random();
        //生成订单标号
        this.orderId = UUID.randomUUID().toString().replaceAll("-","");
        //生成商品价格
        this.productPrice = r.nextInt(1000);
        this.productId = r.nextInt(10)+"";
        //转换成JSON格式
        JSONObject obj = new JSONObject();
        String jsonString = obj.toJSONString(this);
        return  jsonString;
    }
}
