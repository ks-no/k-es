import no.ks.kes.demoapp.Application
import no.ks.kes.demoapp.BasketCmds
import no.ks.kes.demoapp.ShippedBaskets
import org.awaitility.kotlin.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.util.*

@SpringBootTest(classes = [Application::class])
class Test {

    @Test
    @DisplayName("Test that we can checkout a basket, and that this creates a shipment")
    internal fun testCreateShipment(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: ShippedBaskets) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shippedBaskets.getShippedBasket(basketId) } matches { it!!.contains(itemId)}
    }

    @Test
    @DisplayName("Test that adding the same item to a basket multiple times creates a shipment with multiple copies of the item")
    internal fun testCreateShipmentMultiple(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: ShippedBaskets) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shippedBaskets.getShippedBasket(basketId)?.get(itemId) } matches {it == 2}
    }
}