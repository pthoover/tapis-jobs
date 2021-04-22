/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records;


import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.JobQueues;

import java.time.LocalDateTime;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record7;
import org.jooq.Row7;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class JobQueuesRecord extends UpdatableRecordImpl<JobQueuesRecord> implements Record7<Integer, String, Integer, String, String, LocalDateTime, LocalDateTime> {

    private static final long serialVersionUID = -1912542489;

    /**
     * Setter for <code>public.job_queues.id</code>.
     */
    public void setId(Integer value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.job_queues.id</code>.
     */
    public Integer getId() {
        return (Integer) get(0);
    }

    /**
     * Setter for <code>public.job_queues.name</code>.
     */
    public void setName(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.job_queues.name</code>.
     */
    public String getName() {
        return (String) get(1);
    }

    /**
     * Setter for <code>public.job_queues.priority</code>.
     */
    public void setPriority(Integer value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.job_queues.priority</code>.
     */
    public Integer getPriority() {
        return (Integer) get(2);
    }

    /**
     * Setter for <code>public.job_queues.filter</code>.
     */
    public void setFilter(String value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.job_queues.filter</code>.
     */
    public String getFilter() {
        return (String) get(3);
    }

    /**
     * Setter for <code>public.job_queues.uuid</code>.
     */
    public void setUuid(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.job_queues.uuid</code>.
     */
    public String getUuid() {
        return (String) get(4);
    }

    /**
     * Setter for <code>public.job_queues.created</code>.
     */
    public void setCreated(LocalDateTime value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.job_queues.created</code>.
     */
    public LocalDateTime getCreated() {
        return (LocalDateTime) get(5);
    }

    /**
     * Setter for <code>public.job_queues.last_updated</code>.
     */
    public void setLastUpdated(LocalDateTime value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.job_queues.last_updated</code>.
     */
    public LocalDateTime getLastUpdated() {
        return (LocalDateTime) get(6);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<Integer> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record7 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row7<Integer, String, Integer, String, String, LocalDateTime, LocalDateTime> fieldsRow() {
        return (Row7) super.fieldsRow();
    }

    @Override
    public Row7<Integer, String, Integer, String, String, LocalDateTime, LocalDateTime> valuesRow() {
        return (Row7) super.valuesRow();
    }

    @Override
    public Field<Integer> field1() {
        return JobQueues.JOB_QUEUES.ID;
    }

    @Override
    public Field<String> field2() {
        return JobQueues.JOB_QUEUES.NAME;
    }

    @Override
    public Field<Integer> field3() {
        return JobQueues.JOB_QUEUES.PRIORITY;
    }

    @Override
    public Field<String> field4() {
        return JobQueues.JOB_QUEUES.FILTER;
    }

    @Override
    public Field<String> field5() {
        return JobQueues.JOB_QUEUES.UUID;
    }

    @Override
    public Field<LocalDateTime> field6() {
        return JobQueues.JOB_QUEUES.CREATED;
    }

    @Override
    public Field<LocalDateTime> field7() {
        return JobQueues.JOB_QUEUES.LAST_UPDATED;
    }

    @Override
    public Integer component1() {
        return getId();
    }

    @Override
    public String component2() {
        return getName();
    }

    @Override
    public Integer component3() {
        return getPriority();
    }

    @Override
    public String component4() {
        return getFilter();
    }

    @Override
    public String component5() {
        return getUuid();
    }

    @Override
    public LocalDateTime component6() {
        return getCreated();
    }

    @Override
    public LocalDateTime component7() {
        return getLastUpdated();
    }

    @Override
    public Integer value1() {
        return getId();
    }

    @Override
    public String value2() {
        return getName();
    }

    @Override
    public Integer value3() {
        return getPriority();
    }

    @Override
    public String value4() {
        return getFilter();
    }

    @Override
    public String value5() {
        return getUuid();
    }

    @Override
    public LocalDateTime value6() {
        return getCreated();
    }

    @Override
    public LocalDateTime value7() {
        return getLastUpdated();
    }

    @Override
    public JobQueuesRecord value1(Integer value) {
        setId(value);
        return this;
    }

    @Override
    public JobQueuesRecord value2(String value) {
        setName(value);
        return this;
    }

    @Override
    public JobQueuesRecord value3(Integer value) {
        setPriority(value);
        return this;
    }

    @Override
    public JobQueuesRecord value4(String value) {
        setFilter(value);
        return this;
    }

    @Override
    public JobQueuesRecord value5(String value) {
        setUuid(value);
        return this;
    }

    @Override
    public JobQueuesRecord value6(LocalDateTime value) {
        setCreated(value);
        return this;
    }

    @Override
    public JobQueuesRecord value7(LocalDateTime value) {
        setLastUpdated(value);
        return this;
    }

    @Override
    public JobQueuesRecord values(Integer value1, String value2, Integer value3, String value4, String value5, LocalDateTime value6, LocalDateTime value7) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        value6(value6);
        value7(value7);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached JobQueuesRecord
     */
    public JobQueuesRecord() {
        super(JobQueues.JOB_QUEUES);
    }

    /**
     * Create a detached, initialised JobQueuesRecord
     */
    public JobQueuesRecord(Integer id, String name, Integer priority, String filter, String uuid, LocalDateTime created, LocalDateTime lastUpdated) {
        super(JobQueues.JOB_QUEUES);

        set(0, id);
        set(1, name);
        set(2, priority);
        set(3, filter);
        set(4, uuid);
        set(5, created);
        set(6, lastUpdated);
    }
}
