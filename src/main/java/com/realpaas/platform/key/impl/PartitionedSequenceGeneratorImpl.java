/**
 * Copyright (c) 2012, RealPaaS Technologies, Ltd. All rights reserved.
 */
package com.realpaas.platform.key.impl;

import com.realpaas.platform.key.KeyedSequenceGenerator;
import com.realpaas.platform.key.PartitionedSequenceGenerator;

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
public class PartitionedSequenceGeneratorImpl implements PartitionedSequenceGenerator {

    public static final String DEFAULT_PARTITION_KEY = "default";
    
    private String partitionKey = DEFAULT_PARTITION_KEY;
    
    private KeyedSequenceGenerator keyedSequenceGenerator;
    
    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public void setKeyedSequenceGenerator(KeyedSequenceGenerator keyedSequenceGenerator) {
        this.keyedSequenceGenerator = keyedSequenceGenerator;
    }

    @Override
    public long nextValue() {
        return keyedSequenceGenerator.nextValue( partitionKey );
    }

    @Override
    public long nextValue(String key) {
        return keyedSequenceGenerator.nextValue( partitionKey + "." + key );
    }
    
}
