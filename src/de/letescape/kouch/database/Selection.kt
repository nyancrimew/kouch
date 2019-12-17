package de.letescape.kouch.database

import com.google.gson.JsonObject
import com.google.gson.annotations.SerializedName
import de.letescape.kouch.documents.Document
import kotlin.reflect.KProperty1

data class  Selection(
    val selector: Map<String, *>,
    val limit: Int? = null,
    val skip: Int? = null,
    val sort: List<Map<String, String>>? = null,
    val fields: List<String>? = null,
    @SerializedName("use_index")
    val useIndex: List<String>? = null,
    val bookmark: String? = null,
    val update: Boolean? = null,
    val stable: Boolean? = null,
    val stale: String? = null
)

data class SelectionResult<T: Document<T>>(
    val docs: List<T>,
    val warning: String?,
    val bookmark: String
)

class SelectionBuilder<T : Document<T>> {
    private val selector = mutableMapOf<String, MutableMap<String, Any?>>()
    private val sort = mutableListOf<Map<String, String>>()
    var fields: List<String>? = null
    var limit: Int? = null
    var skip: Int? = null
    val useIndex: List<String>? = null
    val bookmark: String? = null
    val update: Boolean? = null
    val stable: Boolean? = null
    val stale: String? = null

    infix fun <V> KProperty1<T, V>.eq(value: V) {
        addSelector(name, "eq", value)
    }

    infix fun <V> KProperty1<T, V>.lt(value: V) {
        addSelector(name, "lt", value)
    }

    infix fun <V> KProperty1<T, V>.lte(value: V) {
        addSelector(name, "lte", value)
    }

    infix fun <V> KProperty1<T, V>.ne(value: V) {
        addSelector(name, "ne", value)
    }

    infix fun <V> KProperty1<T, V>.gte(value: V) {
        addSelector(name, "gte", value)
    }

    infix fun <V> KProperty1<T, V>.gt(value: V) {
        addSelector(name, "gt", value)
    }

    infix fun KProperty1<T, String>.regex(regex: String) {
        addSelector(name, "regex", regex)
    }

    infix fun KProperty1<T, *>.sort(direction: Direction) {
        sort.add(mapOf(name to direction.string))
    }

    private fun addSelector(field: String, operator: String, value: Any?) {
        if (selector.containsKey(field)) {
            selector[field]!!["\$$operator"] = value
        } else {
            selector[field] = mutableMapOf("\$$operator" to value)
        }
    }

    internal fun build(): Selection = Selection(
        selector,
        limit,
        skip,
        if (sort.isEmpty()) null else sort,
        fields,
        useIndex,
        bookmark,
        update,
        stable,
        stale
    )

    inline val asc get() = Direction.ASC
    inline val desc get() = Direction.DESC

    enum class Direction(val string: String) {
        ASC("asc"),
        DESC("desc")
    }
}