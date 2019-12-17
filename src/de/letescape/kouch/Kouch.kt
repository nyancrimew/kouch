package de.letescape.kouch

import de.letescape.kouch.database.Database
import de.letescape.kouch.util.subPath
import io.ktor.client.HttpClient
import io.ktor.client.features.json.GsonSerializer
import io.ktor.client.features.json.Json
import io.ktor.client.request.head
import io.ktor.http.HttpStatusCode
import io.ktor.http.URLBuilder
import io.ktor.http.Url
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class Kouch(
    val server: Url = Url("http://localhost:5984"),
    val client: HttpClient = defaultClient()
) {
    suspend fun containsDb(name: String): Boolean = withContext(Dispatchers.IO) {
        kotlin.runCatching { client.head<HttpStatusCode>(serverUrl {
            subPath(name)
        }) == HttpStatusCode.OK }.getOrElse { false }
    }

    suspend fun getOrCreateDb(name: String, shards: Int? = null, replicas: Int? = null) = if (containsDb(name)) {
        Database(this, name)
    } else Database.create(this, name, shards, replicas)

    @PublishedApi
    internal inline fun serverUrl(crossinline block: URLBuilder.() -> Unit): Url {
        return URLBuilder(server).apply(block).build()
    }

    companion object {
        fun defaultClient() = HttpClient {
            Json {
                serializer = GsonSerializer()
            }
        }
    }
}

data class OperationResponse(
    val ok: Boolean = false,
    val error: String?,
    val reason: String?
) {
    fun asException() = KouchException(error, reason)
}

class KouchException(error: String?, reason: String?): Exception("$error: $reason")