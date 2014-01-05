/**
 * Copyright (c) 2011, RealPaaS Technologies Ltd. All rights reserved.
 */
package com.realpaas.platform.internal.dataobject;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

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
 *  Mar 18, 2011    henry leu   Create the class
 * </dd>
 * <dd>
 *  Sep 29, 2011    henry leu   Rename the class and Table
 * </dd>
 * 
 * </dl>
 * @author	henry leu
 */

@Entity
@Table(name="PLF_SEQUENCE_REGISTRY")
public class SequenceEntry implements Serializable {

    private static final long serialVersionUID = 1886465913241444165L;

    private String sequenceName;
    
    private long sequenceValue;

    private long sequenceVersion;

    @Id
    @Column(name = "SEQ_NAME", insertable = true, updatable = false, nullable = false, unique = true, length = 200)
    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    @Column(name = "SEQ_VALUE", insertable = true, updatable = true, nullable = false)
    public long getSequenceValue() {
        return sequenceValue;
    }

    public void setSequenceValue(long sequenceValue) {
        this.sequenceValue = sequenceValue;
    }

    @Column(name = "SEQ_VERSION", insertable = true, updatable = true, nullable = false)
    public long getSequenceVersion() {
        return sequenceVersion;
    }

    public void setSequenceVersion(long sequenceVersion) {
        this.sequenceVersion = sequenceVersion;
    }
    
}
