package org.dbsp.util;

import java.util.List;

/** Identity shuffle */
public class IdShuffle implements Shuffle {
    @Override
    public <T> List<T> shuffle(List<T> data) {
        return data;
    }
}
