package com.wxmimperio.file.common

import java.util.ResourceBundle

object PropertiesUtils {

    def loadProperties(): ResourceBundle = {
        ResourceBundle.getBundle("application")
    }
}
