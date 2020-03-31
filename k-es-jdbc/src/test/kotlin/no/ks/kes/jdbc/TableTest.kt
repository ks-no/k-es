package no.ks.kes.jdbc

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class TableTest : StringSpec() {
    init {
        "Test that we can correctly resolve qualified table name" {
            object : Table() {
                override val tableName = "sometable"
            }.apply {
                qualifiedName("someschema") shouldBe "someschema.sometable"
                qualifiedName(null) shouldBe "sometable"
            }
        }

    }
}