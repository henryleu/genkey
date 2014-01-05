/**
 * Copyright (c) 2011, RealPaas Technologies Ltd. All rights reserved.
 */
package com.realpaas.platform.key;

import java.util.HashMap;
import java.util.Map;

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
public final class Base62 {

    private static final int RADIX = 62;
    
    private static final char[] ID62_TABLE = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

    private static final Map<Character, Integer> ID62_MAP = new HashMap<Character, Integer>(100);

    static {
        for(int i=0; i<RADIX; i++){
            ID62_MAP.put( Character.valueOf( ID62_TABLE[i] ), Integer.valueOf( i ) );
        }
    }
    
    private Base62(){}
    
    public static String encode(long id){
        StringBuilder sb = new StringBuilder( 10 );
        int rest = -1;

        while ( id>0 ){
            rest = (int)(id % RADIX);
            id = id / RADIX;
            sb.insert( 0, ID62_TABLE[rest] );
        }
        
        if( -1==rest && 0==id){
            sb.append( ID62_TABLE[0] );
        }
        else if( -1==rest && 0<id){
            throw new IllegalArgumentException( "\"id\" should not be negative" );
        }
        
        return sb.toString();
    }

    public static long decode(String code){
        if(code==null || code.length()==0){
            throw new IllegalArgumentException( "Base62 code is needed as the only argument" );
        }
        
        long id = 0;
        int position = 0;
        long radix = 1;
        for( int i=code.length()-1; i>-1; i--, position++ ){
            Character ch = Character.valueOf( code.charAt( i ) );
            Integer num = ID62_MAP.get( ch );
            if( num==null ){
                throw new IllegalArgumentException( code + " is not legal Base62 code" ); 
            }
            if(position!=0){
                radix = radix * RADIX;
            }
            id += radix * num.longValue();
        }
        
        return id;
    }

    public static boolean isBase62(String code){
        if(code==null || code.length()==0){
            return false;
        }
        int len = code.length();
        for(int i=0; i<len; i++){
            Character ch = Character.valueOf( code.charAt( i ) );
            if( !ID62_MAP.containsKey( ch ) ){
                return false;
            }
        }
        return true;
    }
    
}
