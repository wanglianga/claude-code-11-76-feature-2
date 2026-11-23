package com.labroute.service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * JdbcTemplate.queryForList maps timestamptz columns to java.sql.Timestamp, while
 * RowMapper call sites use getObject(..., OffsetDateTime.class). This helper accepts
 * either so service code can stop caring which path produced the row.
 */
final class Times {

    private Times() {
    }

    static OffsetDateTime toOdt(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof OffsetDateTime odt) {
            return odt;
        }
        if (value instanceof java.sql.Timestamp ts) {
            return ts.toInstant().atOffset(ZoneOffset.UTC);
        }
        if (value instanceof java.util.Date d) {
            return d.toInstant().atOffset(ZoneOffset.UTC);
        }
        return OffsetDateTime.parse(String.valueOf(value));
    }
}
