package org.redisson.kotlin

import org.redisson.api.RCascadeType
import org.redisson.api.RMap
import org.redisson.api.annotation.RCascade
import org.redisson.api.annotation.REntity
import org.redisson.api.annotation.RFieldAccessor
import org.redisson.api.annotation.RId
import org.redisson.api.annotation.RIndex
import org.redisson.liveobject.resolver.LongGenerator
import org.redisson.liveobject.resolver.UUIDGenerator
import java.io.Serializable
import java.util.*
import kotlin.collections.HashMap

object KotlinEntities {

    @REntity
    open class TestEnum : Serializable {
        enum class MyEnum { A, B }

        @RId
        open var id: String? = null
        open var myEnum1: MyEnum? = null
        open var myEnum2: MyEnum? = null
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestREntity() : Comparable<TestREntity>, Serializable {

        @RId
        open var name: String = ""

        open var value: String? = ""

        constructor(name: String, value: String?) : this() {
            this.name = name
            this.value = value
        }

        override operator fun compareTo(other: TestREntity): Int {
            val res = name.compareTo(other.name)
            return if (res == 0) {
                value!!.compareTo(other.value!!)
            } else res
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestREntityWithRMap() : Comparable<TestREntityWithRMap>, Serializable {

        @RId
        open var name: String = ""
            protected set

        open var value: RMap<*, *>? = null

        constructor(name: String, value: RMap<*, *>?) : this() {
            this.name = name
            this.value = value
        }

        override operator fun compareTo(other: TestREntityWithRMap): Int {
            val res = name.compareTo(other.name)
            return if (res == 0 || value != null || other.value != null) {
                if (value!!.name == null) {
                    -1
                } else value!!.name.compareTo(other.value!!.name)
            } else res
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestREntityWithMap() : Comparable<TestREntityWithMap>, Serializable {

        @RId(generator = UUIDGenerator::class)
        open var name: String = ""
            protected set

        open var value: Map<*, *>? = null

        constructor(name: String, value: Map<*, *>?) : this() {
            this.name = name
            this.value = value
        }

        override operator fun compareTo(other: TestREntityWithMap): Int {
            return name.compareTo(other.name)
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestREntityIdNested : Comparable<TestREntityIdNested>, Serializable {

        @RId
        open var name: TestREntity
            protected set

        open var value: String = ""

        constructor(name: TestREntity) {
            this.name = name
        }

        constructor(name: TestREntity, value: String) : this(name) {
            this.value = value
        }

        override operator fun compareTo(other: TestREntityIdNested): Int {
            val res = name.compareTo(other.name)
            return if (res == 0) {
                value.compareTo(other.value)
            } else res
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestREntityValueNested() : Comparable<TestREntityValueNested>, Serializable {
        @RId
        open var name: String = ""
            protected set

        open var value: TestREntityWithRMap? = null

        constructor(name: String) : this() {
            this.name = name
        }

        constructor(name: String, value: TestREntityWithRMap?) : this(name) {
            this.value = value
        }

        override operator fun compareTo(other: TestREntityValueNested): Int {
            val res = name.compareTo(other.name)
            return if (res == 0 || value != null || other.value != null) {
                value!!.compareTo(other.value!!)
            } else res
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestIndexed() : Serializable {

        @RId
        open var id: String? = null
            protected set

        @RIndex
        open var name1: String? = null

        @RIndex
        open var name2: String? = null

        @RIndex
        open var num1: Int? = null

        @RIndex
        open var bool1: Boolean? = null

        @RIndex
        open var obj: TestIndexed? = null

        @RIndex
        open var num2 = 0

        constructor(id: String?) : this() {
            this.id = id
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestClass() {

        @RId(generator = UUIDGenerator::class)
        open var id: Serializable? = null
            protected set

        open var value: String? = null
        open var code: String? = null

        @RCascade(RCascadeType.ALL)
        open var content: Any? = null

        open var values: MutableMap<String, String> = HashMap()

        constructor(id: Serializable?) : this() {
            this.id = id
        }

        @RFieldAccessor
        open fun <T> set(field: String?, value: T) {
        }

        @RFieldAccessor
        open fun <T> get(field: String?): T? {
            return null
        }

        fun addEntry(key: String, value: String) {
            values[key] = value
        }

        override fun equals(obj: Any?): Boolean {
            if (obj == null || obj !is TestClass || this.javaClass != obj.javaClass) {
                return false
            }
            val o = obj
            return (id == o.id
                && code == o.code
                && value == o.value
                && content == o.content)
        }

        override fun hashCode(): Int {
            var hash = 3
            hash = 23 * hash + Objects.hashCode(value)
            hash = 23 * hash + Objects.hashCode(code)
            hash = 23 * hash + Objects.hashCode(id)
            hash = 23 * hash + Objects.hashCode(content)
            return hash
        }
    }

    open class ObjectId() : Serializable {
        var id = 0

        constructor(id: Int) : this() {
            this.id = id
        }

        override fun equals(obj: Any?): Boolean {
            return if (obj !is ObjectId) {
                false
            } else id == obj.id
        }

        override fun hashCode(): Int {
            var hash = 3
            hash = 11 * hash + id
            return hash
        }

        override fun toString(): String {
            return "" + id
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestClassID1() {

        @RId(generator = LongGenerator::class)
        open var name: Long? = null
            protected set

        constructor(name: Long?) : this() {
            this.name = name
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class TestClassID2() {

        @RId(generator = LongGenerator::class)
        open var name: Long? = null
            protected set

        constructor(name: Long?) : this() {
            this.name = name
        }
    }

    @REntity
    open class TestIndexed1 : Serializable {

        @RId
        open var id: String? = null

        open var keywords: List<String> = ArrayList()
    }

    @REntity(fieldTransformation = REntity.TransformationMode.IMPLEMENTATION_BASED)
    @Suppress("LeakingThis")
    open class TestClassNoTransformation() {

        @RId(generator = UUIDGenerator::class)
        open var id: Serializable? = null
            protected set

        open var value: String? = null
        open var code: String? = null
        open var content: Any? = null

        constructor(id: Serializable?) : this() {
            this.id = id
        }

        override fun equals(obj: Any?): Boolean {
            if (obj == null || obj !is TestClass || this.javaClass != obj.javaClass) {
                return false
            }
            val o = obj
            return (id == o.id
                && code == o.code
                && value == o.value
                && content == o.content)
        }

        override fun hashCode(): Int {
            var hash = 3
            hash = 33 * hash + Objects.hashCode(value)
            hash = 33 * hash + Objects.hashCode(code)
            hash = 33 * hash + Objects.hashCode(id)
            hash = 33 * hash + Objects.hashCode(content)
            return hash
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class MyObject() : Serializable {

        @RId(generator = LongGenerator::class)
        open var id: Long? = null
            protected set

        open var myId: Long? = null
            protected set

        open var name: String? = null

        constructor(myId: Long?) : this() {
            this.myId = myId
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class SimpleObject() {

        @RId(generator = UUIDGenerator::class)
        open var id: String? = null
            protected set

        open var value: Long? = null
    }

    @REntity
    @Suppress("LeakingThis")
    open class ObjectWithList() {

        @RId(generator = UUIDGenerator::class)
        open var id: String? = null
            protected set

        open var objects: List<SimpleObject>? = null
        open var so: SimpleObject? = null
    }

    @REntity
    @Suppress("LeakingThis")
    open class Customer() {

        @RId
        open var id: String? = null
            protected set

        @RCascade(RCascadeType.ALL)
        open var orders: MutableList<Order> = mutableListOf()

        constructor(id: String?) : this() {
            this.id = id
        }

        fun addOrder(order: Order) {
            orders.add(order)
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class Order() {

        @RId(generator = LongGenerator::class)
        open var id: Long? = null
            protected set

        @RCascade(RCascadeType.PERSIST, RCascadeType.DETACH)
        open var customer: Customer? = null

        constructor(customer: Customer?) : this() {
            this.customer = customer
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class SetterEncapsulation() {

        @RId(generator = LongGenerator::class)
        open var id: Long? = null
            protected set

        protected open var map: MutableMap<String, Int>? = null

        fun getItem(name: String): Int? {
            return map!![name]
        }

        fun addItem(name: String, amount: Int) {
            map!![name] = amount
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class ClassWithoutIdSetterGetter() {

        @RId(generator = LongGenerator::class)
        protected open var id: Long? = null

        open var name: String? = null
            protected set

        constructor(name: String?) : this() {
            this.name = name
        }
    }

    @REntity
    @Suppress("LeakingThis")
    open class Animal() {

        @RId(generator = LongGenerator::class)
        protected open var id: Long? = null

        open var name: String? = null
            protected set

        constructor(name: String?) : this() {
            this.name = name
        }
    }

    open class Dog : Animal {
        open var breed: String? = null

        constructor() : super() {}
        constructor(name: String?) : super(name) {}
    }

    open class MyCustomer : Customer {

        @RCascade(RCascadeType.ALL)
        open var specialOrders: MutableList<Order> = ArrayList()

        constructor() : super() {}
        constructor(id: String?) : super(id) {}

        fun addSpecialOrder(order: Order) {
            specialOrders.add(order)
        }
    }

    open class MyObjectWithList() : ObjectWithList() {
        open var name: String? = null
    }

    @REntity
    open class HasIsAccessor {

        @RId(generator = LongGenerator::class)
        protected open var id: Long? = null

        open var good = false
    }

}
