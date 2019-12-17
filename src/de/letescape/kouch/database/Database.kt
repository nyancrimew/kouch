package de.letescape.kouch.database

import com.google.gson.annotations.SerializedName
import de.letescape.kouch.Kouch
import de.letescape.kouch.OperationResponse
import de.letescape.kouch.documents.Document
import de.letescape.kouch.documents.DocumentCompanion
import de.letescape.kouch.util.subPath
import io.ktor.client.features.json.defaultSerializer
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.response.HttpResponse
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import kotlin.reflect.KProperty1

class Database(
    @PublishedApi
    internal val kouch: Kouch,
    val name: String
) {
    private val dbJob = SupervisorJob()

    suspend fun getInfo(): DatabaseInfo =
        withContext(Dispatchers.IO) { kouch.client.get<DatabaseInfo>(kouch.serverUrl { subPath(name) }) }

    suspend fun createDocument(document: Document<*>, batch: Boolean = false): DocumentUpdateResponse =
        withContext(Dispatchers.IO + dbJob) {
            document.validate()
            kouch.client.post<DocumentUpdateResponse>(kouch.serverUrl {
                subPath(name)
                if (batch) {
                    parameters["batch"] = "ok"
                }
            }) {
                body = defaultSerializer().write(document)
            }
        }

    suspend fun updateDocument(document: Document<*>, batch: Boolean = false): DocumentUpdateResponse =
        withContext(Dispatchers.IO + dbJob) {
            document.validate()
            kouch.client.put<DocumentUpdateResponse>(kouch.serverUrl {
                subPath(name, document.id!!)
                if (batch) {
                    parameters["batch"] = "ok"
                }
            }) {
                body = defaultSerializer().write(document)
            }
        }

    suspend fun ensureFullCommit() = withContext(Dispatchers.IO) {
        kouch.client.post<HttpResponse>(kouch.serverUrl { subPath(name, "_ensure_full_commit") })
        Unit
    }

    suspend inline fun <reified T : Document<T>> findById(id: String): T? = withContext(Dispatchers.IO) {
        runCatching { kouch.client.get<T>(kouch.serverUrl { subPath(name, id) }) }.getOrNull()
    }

    suspend fun <T : Document<T>> find(selector: suspend SelectionBuilder<T>.() -> Unit): List<T> {
        val builder = SelectionBuilder<T>()
        selector(builder)
        val result = runCatching { kouch.client.get<SelectionResult<T>>(kouch.serverUrl { subPath(name, "_find") }) {
            body = builder.build()
        } }.getOrNull()

        return result?.docs ?: emptyList()
    }

    suspend inline fun <T> scoped(batch: Boolean = false, crossinline block: suspend DatabaseScope.() -> T): T =
        coroutineScope {
            val res = block(DatabaseScope(batch, this@Database))
            if (batch) {
                ensureFullCommit()
            }
            return@coroutineScope res
        }

    companion object {
        suspend fun create(kouch: Kouch, name: String, shards: Int? = null, replicas: Int? = null): Database =
            withContext(Dispatchers.IO) {
                val response = kouch.client.put<OperationResponse>(kouch.serverUrl {
                    subPath(name)
                    if (shards != null) {
                        parameters["q"] = shards.toString()
                    }
                    if (replicas != null) {
                        parameters["n"] = replicas.toString()
                    }
                })
                if (!response.ok) {
                    throw response.asException()
                }
                Database(kouch, name)
            }
    }

}

data class DocumentUpdateResponse(
    val id: String,
    val ok: Boolean,
    val rev: String
)

class DatabaseScope(val batch: Boolean, val db: Database) {
    suspend inline fun <T : Document<T>> Document<T>.create() = create(db, batch)
    suspend inline fun <T : Document<T>> Document<T>.update() = update(db, batch)
    suspend inline fun <T : Document<T>> Document<T>.update(crossinline block: suspend T.() -> Unit) =
        update(db, batch, block)
    suspend inline fun <reified T : Document<T>> DocumentCompanion<T>.findById(id: String) = db.findById<T>(id)
    suspend inline fun <T: Document<T>> DocumentCompanion<T>.find(noinline selector: suspend SelectionBuilder<T>.() -> Unit) = db.find(selector)
}

data class DatabaseInfo(
    val cluster: DbCluster,
    @SerializedName("compact_running")
    val compactRunning: Boolean,
    @SerializedName("db_name")
    val name: String,
    @SerializedName("disk_format_version")
    val diskFormatVersion: Int,
    @SerializedName("doc_count")
    val documentCount: Int,
    @SerializedName("doc_del_count")
    val deletedDocumentCount: Int,
    val sizes: Sizes
)

data class DbCluster(
    @SerializedName("n")
    val replicas: Int,
    @SerializedName("q")
    val shards: Int,
    @SerializedName("r")
    val readQuorum: Int,
    @SerializedName("w")
    val writeQuorum: Int
)

data class Sizes(
    val active: Long,
    val external: Long,
    val file: Long
)