/*
 * Created on Jun 27, 2005
 *
 * copyright ill.com 2005
 */
package com.guba.mogilefs.test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author ericlambrecht
 *  
 */
public class URITest {

    public static void main(String[] args) {
        try {
            URI uri = new URI("//somehost.somewhere.com:800");

            System.out.println(" parsed " + uri.toString());
            System.out.println(" authority is " + uri.getAuthority());
            System.out.println(" port is " + uri.getPort());
            System.out.println(" host is " + uri.getHost());

        } catch (URISyntaxException e) {
            System.out.println("syntax exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
