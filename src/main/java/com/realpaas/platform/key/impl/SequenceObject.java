/**
 * Copyright (c) 2012, RealPaaS Technologies, Ltd. All rights reserved.
 */
package com.realpaas.platform.key.impl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
public class SequenceObject {
    private final String key;
    
    private final AtomicLong pointer;
    
    private final AtomicLong valve;
    
    private final AtomicBoolean loaded;
    
    private final int increment;
    
    public SequenceObject(String key, long pointer, long valve) {
        super();
        this.key = key;
        this.pointer = new AtomicLong( pointer );
        this.valve = new AtomicLong( valve );
        this.loaded = new AtomicBoolean( false );
        this.increment = 100;
    }
    
    public SequenceObject(String key, long pointer, long valve, int increment) {
        super();
        this.key = key;
        this.pointer = new AtomicLong( pointer );
        this.valve = new AtomicLong( valve );
        this.loaded = new AtomicBoolean( false );
        this.increment = increment;
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
    
    public int getIncrement() {
        return increment;
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
