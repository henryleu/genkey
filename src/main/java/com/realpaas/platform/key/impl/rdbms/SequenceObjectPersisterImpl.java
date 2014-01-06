/**
 * Copyright (c) 2012, RealPaaS Technologies, Ltd. All rights reserved.
 */
package com.realpaas.platform.key.impl.rdbms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.realpaas.platform.key.SequenceGeneratorException;
import com.realpaas.platform.key.impl.SequenceObject;
import com.realpaas.platform.key.impl.SequenceObjectPersister;

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
 * 	2014-1-6	henryleu	Create the class
 * </dd>
 * 
 * </dl>
 * @author	henryleu Email/MSN: hongli_leu@126.com
 */
public class SequenceObjectPersisterImpl implements SequenceObjectPersister{

    private static final String SEQUENCE_TABLE_NAME = "PLF_SEQUENCE_REGISTRY";
    private static final String SEQUENCE_NAME = "SEQ_NAME";
    private static final String SEQUENCE_VALUE = "SEQ_VALUE";
    private static final String SEQUENCE_VERSION = "SEQ_VERSION";
    
    private DataSource dataSource;
    private String sequenceSchemaName = "";
    private String sequenceTableName = SEQUENCE_TABLE_NAME;
    
    private String insertSql;
    private String updateSql;
    private String selectSql;
    
    private final Log logger = LogFactory.getLog(getClass());

    private boolean disableLogging = true;


    public void init(){
        insertSql = makeInsertSql();
        updateSql = makeUpdateSql();
        selectSql = makeSelectSql();        
    }
    
    @Override
    public SequenceObject loadSequenceObject(String storedKey) {
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

    @Override
    public void createSequenceObject(String storedKey, Long value) {
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

    @Override
    public void updateSequenceObject(String storedKey, SequenceObject cachedSo) {
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
                so = new SequenceObject( storedKey, rs.getLong(1), rs.getLong(1) ); //TODO: set increment value loaded from storage
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
            
            long valve = so.step( so.getIncrement() ); //TODO: remove the parameter instead of calculating it in SequenceObject internally
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
    
    public void setDisableLogging(boolean disableLogging) {
        this.disableLogging = disableLogging;
    }
    
    private boolean isDisableLogging() {
        return disableLogging;
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
