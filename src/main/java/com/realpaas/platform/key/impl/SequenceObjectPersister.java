/**
 * Copyright (c) 2012, RealPaaS Technologies, Ltd. All rights reserved.
 */
package com.realpaas.platform.key.impl;

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
public interface SequenceObjectPersister {
    
    /**
     * Load a sequence object info in Storage, and create a sequence object with the loaded info,
     * then the object.
     * @param storedKey sequence key in storage
     */
    public SequenceObject loadSequenceObject(String storedKey);
    
    /**
     * Crate a new sequence object info in Storage with the sequence key and initial value
     * @param storedKey
     * @param value
     */
    public void createSequenceObject(String storedKey, Long value);
    
    /**
     * Get and update sequence object in DB, and copy to sequence object in Cache  
     * @param storedKey
     * @param cachedSo
     */
    public void updateSequenceObject(String storedKey, SequenceObject cachedSo);
}
