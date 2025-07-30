import org.junit.Assert;
import org.junit.Test;

public class IntegratorTests {
    int sum(int[] data) {
        int sum = 0;
        for (int i = 0; i < data.length; i++) {
            sum += data[i];
        }
        return sum;
    }

    int sum2(int[] data) {
        int[] sum = new int[data.length + 1];
        sum[0] = 0;
        for (int i = 0; i < data.length; i++) {
            sum[i + 1] = sum[i] + data[i];
        }
        return sum[data.length];
    }

    @Test
    public void testSum() {
        int[] data = new int[] { 1, 2, 3, -2, -1 };
        int sum = this.sum(data);
        int sum2 = this.sum2(data);
        Assert.assertEquals(sum, sum2);
    }
}
