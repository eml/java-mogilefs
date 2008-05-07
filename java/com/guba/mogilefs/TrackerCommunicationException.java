/*
 * Created on Jun 27, 2005
 *
 * copyright ill.com 2005
 */
package com.guba.mogilefs;

/**
 * This exception means we got a garbled response from a tracker. Normally this
 * shouldn't happen, and it indicates something is wrong with the tracker or
 * with our communication link to the tracker. The message should contain the
 * address of the tracker
 * 
 * @author ericlambrecht
 *  
 */
public class TrackerCommunicationException extends MogileException {

    private static final long serialVersionUID = 8151266363306339465L;

    public TrackerCommunicationException(String message) {
        super(message);
    }

    public TrackerCommunicationException(String message, Throwable t) {
        super(message, t);
    }
}
