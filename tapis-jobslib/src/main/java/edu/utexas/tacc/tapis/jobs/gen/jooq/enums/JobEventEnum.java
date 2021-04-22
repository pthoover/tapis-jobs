/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.jobs.gen.jooq.enums;


import edu.utexas.tacc.tapis.jobs.gen.jooq.Public;

import org.jooq.Catalog;
import org.jooq.EnumType;
import org.jooq.Schema;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public enum JobEventEnum implements EnumType {

    JOB_NEW_STATUS("JOB_NEW_STATUS"),

    JOB_INPUT_TRANSACTION_ID("JOB_INPUT_TRANSACTION_ID"),

    JOB_ARCHIVE_TRANSACTION_ID("JOB_ARCHIVE_TRANSACTION_ID"),

    JOB_ERROR_MESSAGE("JOB_ERROR_MESSAGE");

    private final String literal;

    private JobEventEnum(String literal) {
        this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
        return getSchema() == null ? null : getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
        return Public.PUBLIC;
    }

    @Override
    public String getName() {
        return "job_event_enum";
    }

    @Override
    public String getLiteral() {
        return literal;
    }
}
