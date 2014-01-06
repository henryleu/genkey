/**
 * Copyright (c) 2012, RealPaaS Technologies, Ltd. All rights reserved.
 */
package com.realpaas.platform.key.impl;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.realpaas.platform.key.KeyedSequenceGenerator;
import com.realpaas.platform.key.SequenceGeneratorException;

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
    
    private long initValue = DEFAULT_INIT_VALUE;
    private int increment = DEFAULT_INCREMENT;
    private int preIncrement = DEFAULT_INCREMENT / 2;

    private boolean disableLogging = true;
    private ConcurrentMap<String, SequenceObject> sequenceCache;

    private final Log logger = LogFactory.getLog(getClass());
    private final Random random = new Random( System.currentTimeMillis() );
    private final static int attemptTimes = 3;
    private final static int constantMillisBeforeAttempt = 100;
    private final static int maxRandomMillisBeforeAttempt = 500;
    
    private SequenceObjectPersister persister;
    
    public KeyedSequenceGeneratorImpl() {
        sequenceCache = new ConcurrentHashMap<String, SequenceObject>();
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
//                SequenceObject storedSo = getSequenceObject(storedKey);
                SequenceObject storedSo = persister.loadSequenceObject( storedKey );
                if(storedSo == null) {
                    
                    /*
                     * Create the SequenceObject of the key in DB
                     */
//                    createSequenceObject( storedKey, so.getValve() );
                    persister.createSequenceObject( storedKey, so.getValve() );
                }
                else {
                    /*
                     * Get and update the SequenceObject of the key when loading it from key
                     * table in DB since last time platform reset
                     */
//                    updateSequenceObject( storedKey, storedSo );
                    persister.updateSequenceObject( storedKey, storedSo );
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
//                    updateSequenceObject( storedKey, so );
                    persister.updateSequenceObject( storedKey, so );
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
    

    public void setPersister(SequenceObjectPersister persister) {
        this.persister = persister;
    }
    
}
