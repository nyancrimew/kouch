package de.letescape.kouch.util

import io.ktor.http.URLBuilder

fun URLBuilder.subPath(vararg components: String) {
    path(encodedPath.split('/').filterNot { it.isBlank() } + components)
}