/**
 * Copyright (c) 2011, RealPaaS Technologies Ltd. All rights reserved.
 */
package com.realpaas.platform.key;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.realpaas.platform.test.AbstractMetricsTask;
import com.realpaas.platform.test.AbstractTest;
import com.realpaas.platform.test.BeanContainer;
import com.realpaas.platform.test.ConcurrentMetricsTest;
import com.realpaas.platform.test.MetricsTaskFactory;

/**
 * <p>
 * 
 * <dl>
 * <dt><b>Examples:</b></dt>
 * <p>
 * 
 * <pre>
 * </pre>
 * 
 * <p>
 * <dt><b>Thread Safety:</b></dt>
 * <dd> <b>NOT-THREAD-SAFE</b> and <b>NOT-APPLICABLE</b> (for it will never be
 * used on multi-thread occasion.) </dd>
 * 
 * <p>
 * <dt><b>Serialization:</b></dt>
 * <dd> <b>NOT-SERIALIIZABLE</b> and <b>NOT-APPLICABLE</b> (for it have no
 * need to be serializable.) </dd>
 * 
 * <p>
 * <dt><b>Design Patterns:</b></dt>
 * <dd>
 * 
 * </dd>
 * 
 * <p>
 * <dt><b>Change History:</b></dt>
 * <dd> Date Author Action </dd>
 * <dd> Jul 13, 2012 Henry.Lv Create the class </dd>
 * 
 * </dl>
 * 
 * @author Henry.Lv MSN/Email: hongli_leu@126.com
 * @see
 * @see
 */
public class KeyedSequenceGeneratorImplTest extends AbstractTest{
    KeyedSequenceGenerator keyedSequenceGenerator;

    @Override
    public void setUp() throws Exception {
        keyedSequenceGenerator = (KeyedSequenceGenerator) BeanContainer.i().getBean( "keyedSequenceGenerator" );
        assertNotNull( keyedSequenceGenerator );
    }

    @Override
    public void tearDown() throws Exception {
        
    }
    
    public static void main(String[] args){
        KeyedSequenceGeneratorImplTest testClass = new KeyedSequenceGeneratorImplTest(); 
        testClass.nextValue();
    }

    @Test(groups = { "platform", "key" })
    public void nextValue() {
        final Map<Long, String> sequenceMap = new ConcurrentHashMap<Long, String>();
        final int threadCount = 50;
        final long valueCount = 20;
        final AtomicInteger counter = new AtomicInteger(1);
        
        class GetSequenceTask extends AbstractMetricsTask{
            
            @Override
            protected void execute() {
                for(int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    try {
                        long seq = keyedSequenceGenerator.nextValue("test");
                        StringBuilder sbMsg = new StringBuilder();
                        sbMsg.append( counter.getAndIncrement() ).append( ": ID = " ).append( seq ).append( ", ThreadId=" ).append( Thread.currentThread().getName() );
//                        System.out.println( "============ " + sbMsg.toString() );
                        if(sequenceMap.containsKey( seq )) {
                            assertTrue( false );
                        }
                        sequenceMap.put( seq, Thread.currentThread().getName() );
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            protected void processResult() {
                //System.out.println( "It took " + took + " milliseconds to run the task" );
            }
        }
        
        class GetSequenceTaskFactory implements MetricsTaskFactory{

            @Override
            public AbstractMetricsTask newMetricsTask() {
                return new GetSequenceTask();
            }

            @Override
            public AbstractMetricsTask newMetricsTask(AtomicLong total) {
                AbstractMetricsTask task = new GetSequenceTask();
                task.setTotal( total ); 
                return task; 
            }
        }

        ConcurrentMetricsTest getSequenceCmt = new ConcurrentMetricsTest("GetSequence", threadCount, new GetSequenceTaskFactory());
        getSequenceCmt.runAndWait();
    }
    
}
