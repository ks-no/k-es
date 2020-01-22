import junit.framework.Assert.assertTrue
import no.ks.kes.demoapp.Application
import no.ks.kes.demoapp.BasketCmds
import no.ks.kes.demoapp.ShippedBaskets
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.util.*

@SpringBootTest(classes = [Application::class])
class Test {

    @Test
    internal fun name(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: ShippedBaskets) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.StartSession(basketId))
        basketCmds.handle(BasketCmds.AddItemToBasket(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOutBasket(basketId))
        Thread.sleep(1000)
        Thread.sleep(10000)
        assertTrue(shippedBaskets.getShippedBasket(basketId)!!.contains(itemId))
    }
}