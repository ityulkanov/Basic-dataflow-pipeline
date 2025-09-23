package com.ityulkanov.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {

    private ExceptionUtil(){}
    public static String getExceptionAsString(Throwable e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
