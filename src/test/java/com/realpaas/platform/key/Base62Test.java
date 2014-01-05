/**
 * Copyright (c) 2011, RealPaas Technologies Ltd. All rights reserved.
 */
package com.realpaas.platform.key;

import org.testng.annotations.Test;

import com.realpaas.platform.test.AbstractTest;

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
 * 	Jan 19, 2012	henry leu	Create the class
 * </dd>
 *
 * </dl>
 * @author	henry leu
 * @see
 * @see
 */
public class Base62Test extends AbstractTest{

    @Override
    public void setUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}
    
//    @Test(groups={"all", "ut", "key"})
    public void convert(){
        long id1  = 0L;
        long id2  = 1L;
        long id21  = 9L;
        long id22  = 10L;
        long id3  = 61L;
        long id4  = 62L;
        long id5  = 63L;
        long id6  = 100L;
        long id7  = 1000L;
        long id71 = 10000L;
        long id72 = 100000L;
        long id73 = 1000000L;
        
        long id8  = 1234234323L;
        long id9  = 1000000000000L;     //1 Trillion
        long id10 = 3000000000000L;     //3 Trillion
        long id11 = 100000000000000L;   //100 Trillion
        long id12 = Long.MAX_VALUE;
        
        long id = 0;
        long idConverted = 0;
        String code = null;
        
        id = id1;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id2;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id21;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id22;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id3;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id4;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id5;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id6;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id7;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id71;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id72;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id73;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id8;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id9;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id10;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id11;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        id = id12;
        code = Base62.encode( id ); idConverted = Base62.decode( code ); assertEquals( idConverted, id );
        System.out.println( id +  " -> " + code +  " -> " + idConverted );
        
        System.out.println( "" + (62L*62L*62L*62L*62L) );
    }
    
//    @Test(groups={"all", "ut", "key"})
    public void convertCodePerformance(){
        int times = 100000;
        long startMillis = 0L;
        long endMillis = 0L;
        long took = 0L;
        String[] codeArray = new String[times];
        
        /*
         * encode base62
         */
        startMillis = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            codeArray[i] = Base62.encode( i );
        }
        endMillis = System.currentTimeMillis();
        took = endMillis - startMillis;
        System.out.println( "It took " + took + " Millisencods to covert " + times + " IDs to base62 code" );
        
        /*
         * decode base62
         */
        startMillis = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            Base62.decode( codeArray[i] );
        }
        endMillis = System.currentTimeMillis();
        took = endMillis - startMillis;
        System.out.println( "It took " + took + " Millisencods to covert " + times + " base62 codes to ID" );
        
        /*
         * check if code is base62-code
         */
        startMillis = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            Base62.isBase62( codeArray[i] );
        }
        endMillis = System.currentTimeMillis();
        took = endMillis - startMillis;
        System.out.println( "It took " + took + " Millisencods to check " + times + " base62 codes" );
        
    }
    
    @Test(groups={"all", "ut", "key"})
    public void performance(){
        int count = 20000;
        int index = 10000;
        int times = count-index;
        long startMillis = 0L;
        long endMillis = 0L;
        long took = 0L;
        String[] codeArray = new String[times];
        
        /*
         * encode base62
         */
        startMillis = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            codeArray[i] = Base62.encode( index++ );
        }
        endMillis = System.currentTimeMillis();
        took = endMillis - startMillis;
        System.out.println( "It took " + took + " Millisencods to covert " + times + " IDs to base62 code" );
        
        /*
         * decode base62
         */
        startMillis = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            Base62.decode( codeArray[i] );
        }
        endMillis = System.currentTimeMillis();
        took = endMillis - startMillis;
        System.out.println( "It took " + took + " Millisencods to covert " + times + " base62 codes to ID" );
        
        /*
         * check if code is base62-code
         */
        startMillis = System.currentTimeMillis();
        for(int i = 0; i < times; i++) {
            Base62.isBase62( codeArray[i] );
        }
        endMillis = System.currentTimeMillis();
        took = endMillis - startMillis;
        System.out.println( "It took " + took + " Millisencods to check " + times + " base62 codes" );
        
    }    
    
}
