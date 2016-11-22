package com.epam.training.hadoop.loganalyse;

import com.epam.training.hadoop.loganalyse.counter.Browsers;
import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class BrowserFinder {

    private static Pattern pattern = null;

    private BrowserFinder() {
    }

    static {

        StringBuilder stringBuilder = new StringBuilder();
        String browserName = null;

        for (Browsers b : Browsers.values()) {
            stringBuilder.append(b);
            stringBuilder.append("|");
        }
        stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
        pattern = Pattern.compile(stringBuilder.toString());
    }

    public static EnumSet<Browsers> getBrowsers(String info) {
        String upperInfo = info.toUpperCase();

        Matcher matcher = pattern.matcher(upperInfo);

        EnumSet<Browsers> set = EnumSet.noneOf(Browsers.class);

        String group = null;

        while (matcher.find()) {
            group = matcher.group();
            if (!group.isEmpty()) {
                set.add(Browsers.valueOf(group));
            }
        }

        return set;
    }
}
