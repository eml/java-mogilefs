package com.guba.mogilefs;

import java.util.List;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.log4j.Logger;

public class PoolableBackendFactory implements PoolableObjectFactory {
    
    private Logger log = Logger.getLogger(PoolableBackendFactory.class);
    
    private List trackers;
    
    public PoolableBackendFactory(List trackers) {
        log.debug("new backend factory created");
        
        this.trackers = trackers;
    }

    public Object makeObject() throws Exception {
    	try {
			Backend backend = new Backend(trackers, true);
	    
			if (log.isDebugEnabled())
				log.debug("making object " + backend.toString());
	    
			return backend;
    	} catch (Exception e) {
    		log.debug("problem making backend", e);
    		
    		throw e;
    	}
    }

    public void destroyObject(Object obj) throws Exception {
    	if (log.isDebugEnabled())
    		log.debug("destroying object '" + obj.toString() + "'");
        
        if (obj instanceof Backend) {
            Backend backend = (Backend) obj;
            backend.destroy();
        }
    }

    public boolean validateObject(Object obj) {
        if (obj instanceof Backend) {
        	Backend backend = (Backend) obj;
        	boolean connected = backend.isConnected();
        	
        	if (log.isDebugEnabled()) {
        		if (!connected) {
        			log.debug("validating " + obj.toString() + ". Not valid! Last err was: " + backend.getLastErr());
        		} else {
        			log.debug("validating " + obj.toString() + ". validated");
        		}
        	}
        	
        	return connected;
        }
     
        log.debug("validating non-Backend object");
        return false;
    }

    public void activateObject(Object arg0) throws Exception {
        // nothing to do
    	if (log.isDebugEnabled())
    		log.debug("activating object " + arg0.toString());
    }

    public void passivateObject(Object arg0) throws Exception {
        // nothing to do
    	if (log.isDebugEnabled())
    		log.debug("passivating object" + arg0.toString());
    }
    
}
