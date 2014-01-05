/**
 * Copyright (c) 2012, RealPaaS Technologies, Ltd. All rights reserved.
 */
package com.realpaas.platform.key;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * 
 * <dl>
 * <dt><b>Examples:</b></dt>
 * <p>
 * <pre>
 * 
 * </pre>
 * 
 * <p><dt><b>Immutability:</b></dt> 
 * <dd>
 * 	<b>IMMUTABLE</b> and <b>MUTABLE</b>
 * </dd>
 * 
 * <p><dt><b>Thread Safety:</b></dt> 
 * <dd>
 * 	<b>NOT-THREAD-SAFE</b> and <b>NOT-APPLICABLE</b> (for it will never be used on multi-thread occasion.)
 * </dd>
 * 
 * <p><dt><b>Serialization:</b></dt>
 * <dd>
 * 	<b>NOT-SERIALIIZABLE</b> and <b>NOT-APPLICABLE</b> (for it have no need to be serializable.)
 * </dd>
 * 
 * <p><dt><b>Design Patterns:</b></dt>
 * <dd>
 * 	
 * </dd>
 * 
 * <p><dt><b>Change History:</b></dt>
 * <dd>
 * 	Date		Author		Action
 * </dd>
 * <dd>
 * 	2012-7-13	henryleu	Create the class
 * </dd>
 * 
 * </dl>
 * @author	henryleu Email/MSN: hongli_leu@126.com
 */
public class KeyedSequenceGeneratorImpl implements KeyedSequenceGenerator{
    private static final long DEFAULT_INIT_VALUE = 1;
    private static final int DEFAULT_INCREMENT = 1000;
    private static final int MIN_INCREMENT = 10;
    
    private static final String SEQUENCE_TABLE_NAME = "PLF_SEQUENCE_REGISTRY";
    private static final String SEQUENCE_NAME = "SEQ_NAME";
    private static final String SEQUENCE_VALUE = "SEQ_VALUE";
    private static final String SEQUENCE_VERSION = "SEQ_VERSION";
    
    private DataSource dataSource;
    private String sequenceSchemaName = "";
    private String sequenceTableName = SEQUENCE_TABLE_NAME;
    private long initValue = DEFAULT_INIT_VALUE;
    private int increment = DEFAULT_INCREMENT;
    private int preIncrement = DEFAULT_INCREMENT / 2;

    private boolean disableLogging = true;
    private ConcurrentMap<String, SequenceObject> sequenceCache;

    private String insertSql;
    private String updateSql;
    private String selectSql;
    
    private final Log logger = LogFactory.getLog(getClass());
    private final Random random = new Random( System.currentTimeMillis() );
    private final static int attemptTimes = 3;
    private final static int constantMillisBeforeAttempt = 100;
    private final static int maxRandomMillisBeforeAttempt = 500;
    
    static class SequenceObject {
        private final String key;
        
        private final AtomicLong pointer;
        
        private final AtomicLong valve;
        
        private final AtomicBoolean loaded;
        
        public SequenceObject(String key, long pointer, long valve) {
            super();
            this.key = key;
            this.pointer = new AtomicLong( pointer );
            this.valve = new AtomicLong( valve );
            this.loaded = new AtomicBoolean( false );
        }
        
        public String getKey() {
            return key;
        }
        
        public long getPointer() {
            return pointer.get();
        }
        
        public void setPointer(long pointer) {
            this.pointer.set( pointer );
        }
        
        public long getValve() {
            return valve.get();
        }
        
        public void setValve(long valve) {
            this.valve.set( valve );
        }
        
        public boolean getLoaded() {
            return loaded.get();
        }

        public void setLoaded() {
            loaded.set( true );
        }

        public long nextValue() {
            return pointer.incrementAndGet();
        }
        
        public void syncWith(SequenceObject newSo){
            setPointer( newSo.getPointer() );
            setValve( newSo.getValve() );
        }
        
        /**
         * @param preIncrement
         * @return
         */
        public boolean reachValve(int preIncrement) {
            /*
             * reach valve is value
             * considering multiple thread access, use >= instead of ==
             */
            return pointer.get() == valve.get();
        }
        
        /**
         * step forward: increase the valve with increment and return the result.
         * @param increment a pace to step
         * @return return the new valve value
         */
        public long step(long increment) {
            return valve.addAndGet( increment );
        }
    }
    
    public KeyedSequenceGeneratorImpl() {
        sequenceCache = new ConcurrentHashMap<String, SequenceObject>();
    }
    
    public DataSource getDataSource() {
        return dataSource;
    }
    
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    public String getSequenceSchemaName() {
        return sequenceSchemaName;
    }

    public void setSequenceSchemaName(String sequenceSchemaName) {
        this.sequenceSchemaName = sequenceSchemaName;
    }

    public String getSequenceTableName() {
        return sequenceTableName;
    }

    public void setSequenceTableName(String sequenceTableName) {
        this.sequenceTableName = sequenceTableName;
    }
    
    public long getInitValue() {
        return initValue;
    }
    
    public void setInitValue(long initValue) {
        this.initValue = initValue;
    }
    
    public int getIncrement() {
        return increment;
    }
    
    public void setIncrement(int increment) {
        if(increment< MIN_INCREMENT){
            throw new IllegalArgumentException( "Property \"increment\" [" + increment + "] should be greater than or equal to " + MIN_INCREMENT );
        }
        
        this.increment = increment;
    }
    
    public int getPreIncrement() {
        return preIncrement;
    }
    
    public void setPreIncrement(int preIncrement) {
        this.preIncrement = preIncrement;
    }
    
    public boolean isDisableLogging() {
        return disableLogging;
    }

    public void setDisableLogging(boolean disableLogging) {
        this.disableLogging = disableLogging;
    }

    public void init(){
        insertSql = makeInsertSql();
        updateSql = makeUpdateSql();
        selectSql = makeSelectSql();        
    }
    
    public void afterPropertiesSet() throws Exception {
        init();
    }

    @SuppressWarnings("static-access")
    @Override
    public long nextValue(String key) {
        int waitBeforeAttempt = 0;
        long nextValue = 0;
        
        try {
            nextValue = doGetNextValue( key );
        }
        catch (SequenceGeneratorException e) {
            logger.warn("Fail to attempt to get next value", e);
            for(int i = 1; i < attemptTimes; i++) {
                try {
                    waitBeforeAttempt = constantMillisBeforeAttempt + random.nextInt( maxRandomMillisBeforeAttempt );
                    Thread.currentThread().sleep( waitBeforeAttempt );
                    nextValue = doGetNextValue( key );
                    return nextValue;
                }
                catch (SequenceGeneratorException internalE) {
                    logger.warn("Fail to attempt to get next value", internalE);
                }
                catch (Exception internalE) {
                    logger.warn("Fail to attempt to get next value", internalE);
                }
            }
            throw new SequenceGeneratorException( "After " + attemptTimes + " Attempts, Fail to get next value" );
        }
        catch (Exception e) {
            logger.error("Fail to get next value after tried " + attemptTimes + " times", e);
            throw new SequenceGeneratorException(e);
        }
        
        return nextValue;
    }

    private long doGetNextValue(String key) {
        String storedKey = key;
        SequenceObject so = getOrCreateSequenceObject( storedKey );
        long nextValue = -1;
        
        /*
         * the SequenceObject of the key is not in in Cache, so it need to 
         * be created or loaded from DB
         */
        if( !so.getLoaded() ) {
            synchronized( so ){
                SequenceObject storedSo = getSequenceObject(storedKey);
                if(storedSo == null) {
                    
                    /*
                     * Create the SequenceObject of the key in DB
                     */
                    createSequenceObject( storedKey, so.getValve() );
                }
                else {
                    /*
                     * Get and update the SequenceObject of the key when loading it from key
                     * table in DB since last time platform reset
                     */
                    updateSequenceObject( storedKey, storedSo );
                    so.syncWith( storedSo );
                }
                
                /*
                 * Set loaded flag to true after create/update SequenceObject in DB for 
                 * the first time when platform launches
                 */
                so.setLoaded();
            }
        }
        else { // the SequenceObject of the key has already been in Cache
            /*
             * Check if next value reaches the valve of the key in this
             * pre-increment in advance, if yes, increase and update the valve
             * of the key in Cache and DB.
             */
            if( so.reachValve( getPreIncrement() ) ) {
                synchronized( so ){
                    updateSequenceObject( storedKey, so );
                }
            }
        }

        /*
         * Get current value and roll next
         */
        nextValue = so.nextValue();
        
        if( !isDisableLogging() && logger.isDebugEnabled()) {
            logger.debug("Sequence [ key=\"" + key + "\", value=" + nextValue + " ]");
        }
        
        return nextValue;
    }

    /**
     * if no key-matched SO in Cache, create initial one and put it to Cache if it is absent,
     * or return it directly from Cache.
     * <p>Newly created SO's loaded flag is set to false
     * @param storedKey SO's Key
     * @return SequenceObject object.
     */
    private SequenceObject getOrCreateSequenceObject(final String storedKey) {
        SequenceObject so = sequenceCache.get( storedKey );
        if( so==null ){
            so = instantiateInitialSequenceObject( storedKey );
            SequenceObject previousOne = sequenceCache.putIfAbsent( storedKey, so );
            if( previousOne!=null ){
                so = previousOne;
            }
        }
        return so;
    }
    
    /**
     * Instantiate a initial SO with initial pointer, valve and loaded properties. 
     * @param storedKey SO's Key
     * @return newly created SequenceObject object
     */
    private SequenceObject instantiateInitialSequenceObject(final String storedKey) {
        return new SequenceObject( storedKey, getInitValue(), getInitValue() + getIncrement() );
    }
    
    private SequenceObject getSequenceObject(String storedKey) {
        SequenceObject so = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        boolean autoCommit = true;
        boolean readOnly = false;
        int transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        
        /*
         * Setup connection
         */
        try {
            connection = getDataSource().getConnection();
        }
        catch (SQLException e) {
            String strError = "Fail to get DB Connection : " + e.getMessage();
            logger.error(strError, e);
            throw new SequenceGeneratorException( strError, e );
        }

        try {
            if( !isDisableLogging() && logger.isDebugEnabled() ) {
                logger.debug(selectSql);
            }
            
            /*
             * Prepare transaction
             */
            autoCommit = connection.getAutoCommit();
            readOnly = connection.isReadOnly();
            transactionIsolationLevel = connection.getTransactionIsolation();
            changeTransactionSettings(connection, false, true, Connection.TRANSACTION_READ_UNCOMMITTED);
            
            /*
             * Execute data operations
             */
            preparedStatement = connection.prepareStatement(selectSql);
            preparedStatement.setString(1, storedKey);
            rs = preparedStatement.executeQuery();
            if(rs.next()) {
                so = new SequenceObject( storedKey, rs.getLong(1), rs.getLong(1) );
            }
            else {
                so = null;
            }
            
            /*
             * Commit transaction
             */
            connection.commit();
        }
        catch (SQLException e) {
            StringBuilder sbError = new StringBuilder("Fail to get \"Sequence Entry(");
            String strError = null;
            sbError.append(storedKey).append("): ");
            
            /*
             * Rollback transaction
             */
            try {
                connection.rollback();
            }
            catch (SQLException e1) {
                sbError.append(e1.getMessage());
                strError = sbError.toString();
                logger.error(strError, e1);
                throw new SequenceGeneratorException(strError, e1);
            }
            
            sbError.append(e.getMessage());
            strError = sbError.toString();
            logger.error(strError, e);
            throw new SequenceGeneratorException(strError, e);
        }
        finally {
            /*
             * Restore settings and close resources
             */
            changeTransactionSettings(connection, autoCommit, readOnly, transactionIsolationLevel);
            close(connection, preparedStatement, rs);
        }
        
        return so;
    }
    
    private void createSequenceObject(String storedKey, Long value) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        boolean autoCommit = true;
        boolean readOnly = false;
        int transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;

        /*
         * Setup connection
         */
        try {
            connection = getDataSource().getConnection();
        }
        catch (SQLException e) {
            String strError = "Fail to get DB Connection : " + e.getMessage();
            logger.error(strError, e);
            throw new SequenceGeneratorException( strError, e );
        }
        
        try {
            if( !isDisableLogging() && logger.isDebugEnabled() ) {
                logger.debug(insertSql);
            }
            
            /*
             * Prepare transaction
             */
            autoCommit = connection.getAutoCommit();
            readOnly = connection.isReadOnly();
            transactionIsolationLevel = connection.getTransactionIsolation();
            changeTransactionSettings(connection, false, false, Connection.TRANSACTION_READ_COMMITTED);
            
            /*
             * Execute data operations
             */
            preparedStatement = connection.prepareStatement( insertSql );
            preparedStatement.setString(1, storedKey);
            preparedStatement.setLong(2, value);
            preparedStatement.setLong(3, 0);
            int count = preparedStatement.executeUpdate();
            if(count != 1) {
                StringBuilder sbError = new StringBuilder("Fail to insert \"Sequence Entry(");
                sbError.append(storedKey).append(", ").append(value).append(")");
                throw new SequenceGeneratorException(sbError.toString());
            }
            
            /*
             * Commit transaction
             */
            connection.commit();
        }
        catch (SQLException e) {
            StringBuilder sbError = new StringBuilder("Fail to create \"Sequence Entry(");
            String strError = null;
            sbError.append(storedKey).append("): ");
            
            /*
             * Rollback transaction
             */
            try {
                connection.rollback();
            }
            catch (SQLException e1) {
                sbError.append(e1.getMessage());
                strError = sbError.toString();
                logger.error(strError, e1);
                throw new SequenceGeneratorException(strError, e1);
            }
            
            sbError.append(e.getMessage());
            strError = sbError.toString();
            logger.error(strError, e);
            throw new SequenceGeneratorException(strError, e);
        }
        finally {
            /*
             * Restore settings and close resources
             */
            changeTransactionSettings(connection, autoCommit, readOnly, transactionIsolationLevel);
            close(connection, preparedStatement, null);
        }
    }
    
    
    /**
     * Get and update sequence object in DB, and copy to sequence object in Cache  
     * @param storedKey
     * @param cachedSo
     */
    private void updateSequenceObject(String storedKey, SequenceObject cachedSo) {
        SequenceObject so = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        boolean autoCommit = true;
        boolean readOnly = false;
        int transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;

        /*
         * Setup connection
         */
        try {
            connection = getDataSource().getConnection();
        }
        catch (SQLException e) {
            String strError = "Fail to get DB Connection : " + e.getMessage();
            logger.error(strError, e);
            throw new SequenceGeneratorException( strError, e );
        }
        
        try {
            /*
             * Prepare transaction
             */
            autoCommit = connection.getAutoCommit();
            readOnly = connection.isReadOnly();
            transactionIsolationLevel = connection.getTransactionIsolation();
            changeTransactionSettings(connection, false, false, Connection.TRANSACTION_READ_COMMITTED);

            /*
             * Execute data operations
             */
            if( !isDisableLogging() && logger.isDebugEnabled() ) {
                logger.debug(selectSql);
            }
            preparedStatement = connection.prepareStatement(selectSql);
            preparedStatement.setString(1, storedKey);
            rs = preparedStatement.executeQuery();
            final long version;
            if(rs.next()) {
                so = new SequenceObject( storedKey, rs.getLong(1), rs.getLong(1) );
                version = rs.getLong(2);
            }
            else {
                String strError = "Fail to find Sequence Entry with key \"" + storedKey + "\" in DB";
                logger.error(strError);
                throw new SequenceGeneratorException( strError );
            }
            
            if( !isDisableLogging() && logger.isDebugEnabled() ) {
                logger.debug(updateSql);
            }
            
            long valve = so.step( getIncrement() );
            preparedStatement = connection.prepareStatement(updateSql);
            preparedStatement.setLong(1, valve);
            preparedStatement.setLong(2, version+1);
            preparedStatement.setString(3, storedKey);
            preparedStatement.setLong(4, version);
            
            int count = preparedStatement.executeUpdate();
            if(count != 1) {
                StringBuilder sbError = new StringBuilder("Fail to update \"Sequence Entry(");
                sbError.append(storedKey).append(", ").append(valve).append(")");
                logger.error( sbError.toString() );
                throw new SequenceGeneratorException( sbError.toString() );
            }

            /*
             * Commit transaction
             */
            connection.commit();
        }
        catch (SQLException e) {
            StringBuilder sbError = new StringBuilder("Fail to update \"Sequence Entry(");
            String strError = null;
            sbError.append(storedKey).append("): ");
            
            /*
             * Rollback transaction
             */
            try {
                connection.rollback();
            }
            catch (SQLException e1) {
                sbError.append(e1.getMessage());
                strError = sbError.toString();
                logger.error(strError, e1);
                throw new SequenceGeneratorException(strError, e1);
            }
            
            sbError.append(e.getMessage());
            strError = sbError.toString();
            logger.error(strError, e);
            throw new SequenceGeneratorException(strError, e);
        }
        finally {
            /*
             * Restore settings and close resources
             */
            changeTransactionSettings(connection, autoCommit, readOnly, transactionIsolationLevel);
            close(connection, preparedStatement, rs);
        }
        
        /*
         * Sync SequenceObject between DB and cache
         */
        cachedSo.setPointer( so.getPointer() );
        cachedSo.setValve( so.getValve() );
    }

    private String makeInsertSql(){
        StringBuilder sbSql = new StringBuilder(100);
        sbSql.append("INSERT INTO ");
        if(sequenceSchemaName==null || sequenceSchemaName.trim().equals("")){
            sbSql.append(sequenceTableName);
        }
        else{
            sbSql.append(sequenceSchemaName).append(".").append(sequenceTableName);
        }
        sbSql.append(" ( ");
        sbSql.append(SEQUENCE_NAME);
        sbSql.append(", ");
        sbSql.append(SEQUENCE_VALUE);
        sbSql.append(", ");
        sbSql.append(SEQUENCE_VERSION);
        sbSql.append(" ) VALUES ( ?, ?, ? )");
        return sbSql.toString();
    }

    private String makeUpdateSql(){
        StringBuilder sbSql = new StringBuilder(100);
        sbSql.append("UPDATE ");
        if(sequenceSchemaName==null || sequenceSchemaName.trim().equals("")){
            sbSql.append(sequenceTableName);
        }
        else{
            sbSql.append(sequenceSchemaName).append(".").append(sequenceTableName);
        }
        sbSql.append(" SET ");
        sbSql.append(SEQUENCE_VALUE);
        sbSql.append(" = ?,");
        sbSql.append(SEQUENCE_VERSION);
        sbSql.append(" = ? WHERE ");
        sbSql.append(SEQUENCE_NAME);
        sbSql.append(" LIKE ? AND ");
        sbSql.append(SEQUENCE_VERSION);
        sbSql.append(" = ?");
        return sbSql.toString();
    }

    private String makeSelectSql(){
        StringBuilder sbSql = new StringBuilder(100);
        sbSql.append("SELECT ");
        sbSql.append(SEQUENCE_VALUE).append(", ");
        sbSql.append(SEQUENCE_VERSION);
        sbSql.append(" FROM ");
        if(sequenceSchemaName==null || sequenceSchemaName.trim().equals("")){
            sbSql.append(sequenceTableName);
        }
        else{
            sbSql.append(sequenceSchemaName).append(".").append(sequenceTableName);
        }
        sbSql.append(" WHERE ").append(SEQUENCE_NAME).append(" LIKE ? ");
        return sbSql.toString();
    }

    private void changeTransactionSettings(Connection connection, boolean autoCommit, boolean readOnly, int transactionIsolationLevel) {
        try {
            connection.setTransactionIsolation( transactionIsolationLevel );
            connection.setReadOnly( readOnly );
            connection.setAutoCommit( autoCommit );
        }
        catch (SQLException e) {
            /*
             * ignore/swallow it
             */
            String strError = "Fail to change transaction settings: " + e.getMessage();
            logger.error(strError, e);
        }
    }
    
    private void close(Connection connection, Statement statement, ResultSet rs) {
        try {
            if(rs != null) {
                rs.close();
            }
            
            if(statement != null) {
                statement.close();
            }
            
            if(connection != null) {
                connection.close();
            }
        }
        catch (Exception e) {
            /*
             * ignore/swallow it
             */
            String strError = "Fail to close DB resources: " + e.getMessage();
            logger.error(strError, e);
        }
    }
}
