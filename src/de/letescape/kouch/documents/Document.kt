package de.letescape.kouch.documents

import com.google.gson.Gson
import com.google.gson.annotations.Expose
import com.google.gson.annotations.SerializedName
import de.letescape.kouch.Kouch
import de.letescape.kouch.database.Database
import io.ktor.client.call.TypeInfo
import io.ktor.client.features.json.defaultSerializer
import kotlinx.coroutines.coroutineScope
import javax.print.Doc
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

abstract class Document<T: Document<T>> {
    @SerializedName("_rev", alternate = ["rev"])
    var revision: String? = null
        internal set
    @SerializedName("_id", alternate = ["id"])
    open var id: String? = null
        protected set

    suspend fun create(db: Database, batch: Boolean = false): T = coroutineScope {
        val response = db.createDocument(this@Document, batch)
        id = response.id
        revision = response.rev
        this@Document as T
    }

    suspend fun update(db: Database, batch: Boolean = false): T = coroutineScope {
        val response = db.updateDocument(this@Document, batch)
        id = response.id
        revision = response.rev
        this@Document as T
    }

    suspend inline fun update(db: Database, batch: Boolean = false, crossinline block: suspend T.() -> Unit): T {
        block(this@Document as T)
        return update(db, batch)
    }

    @Throws(DocumentValidationException::class)
    open suspend fun validate() {
    }

    override fun toString(): String {
        return Gson().toJson(this)
    }

    companion object
}

open class DocumentCompanion<T: Document<T>> {}

class DocumentValidationException(message: String): Exception(message)
