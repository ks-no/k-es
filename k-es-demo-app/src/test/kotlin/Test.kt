import no.ks.kes.demoapp.Application
import no.ks.kes.demoapp.BasketCmds
import no.ks.kes.demoapp.Shipments
import no.ks.kes.demoapp.WarehouseManager
import org.awaitility.kotlin.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.lang.IllegalStateException
import java.util.*

@SpringBootTest(classes = [Application::class])
class Test {

    @Test
    @DisplayName("Test that we can checkout a basket, and that this creates a shipment")
    internal fun testCreateShipment(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shippedBaskets.getShipments(basketId) } matches { it!!.contains(itemId)}
    }

    @Test
    @DisplayName("Test that we can checkout a basket, and that this creates a shipment")
    internal fun testCreateShipmentFails(@Autowired basketCmds: BasketCmds, @Autowired shipments: Shipments, @Autowired warehouseManager: WarehouseManager) {
        warehouseManager.failNext()

        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shipments.isFailedShipment(basketId) } matches { it == true }
    }

    @Test
    @DisplayName("Test that adding the same item to a basket multiple times creates a shipment with multiple copies of the item")
    internal fun testCreateShipmentMultipleItems(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))

        await untilCallTo { shippedBaskets.getShipments(basketId)?.get(itemId) } matches {it == 2}
    }

    @Test
    @DisplayName("Test that adding an item to a closed basket fails")
    internal fun testAddItemToClosedBasket(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()
        val itemId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        basketCmds.handle(BasketCmds.AddItem(basketId, itemId))
        basketCmds.handle(BasketCmds.CheckOut(basketId))
        assertThrows<IllegalStateException> {basketCmds.handle(BasketCmds.AddItem(basketId, itemId))}
    }

    @Test
    @DisplayName("Test that checking out a empty basket fails")
    internal fun testCheckOutClosedBasket(@Autowired basketCmds: BasketCmds, @Autowired shippedBaskets: Shipments) {
        val basketId = UUID.randomUUID()

        basketCmds.handle(BasketCmds.Create(basketId))
        assertThrows<IllegalStateException> {basketCmds.handle(BasketCmds.CheckOut(basketId))}
    }
}