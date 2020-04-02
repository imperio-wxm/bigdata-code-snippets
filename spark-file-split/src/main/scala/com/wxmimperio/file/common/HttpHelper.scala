package com.wxmimperio.file.common

import scalaj.http.{Http, HttpRequest}

object HttpHelper {

    def httpGet(url: String): HttpRequest = {
        Http(url)
    }

}
