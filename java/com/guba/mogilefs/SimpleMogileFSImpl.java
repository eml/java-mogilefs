package com.guba.mogilefs;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;

/**
 * This implementation creates a fake object pool and a single
 * connection to a backend. This is most definitely not thread
 * safe, and should really only be used for testing purposes. For all
 * intents and purposes, the PooledMogileFSImpl is what you want to
 * use in all situations.
 * 
 * @author ericlambrecht
 *
 */

public class SimpleMogileFSImpl extends BaseMogileFSImpl {

	public SimpleMogileFSImpl(String domain, String[] trackerStrings) throws BadHostFormatException, NoTrackersException {
		super(domain, trackerStrings);
	}
	
	@Override
	protected ObjectPool buildBackendPool() {
				
		return new ObjectPool() {

			 private Backend backend = null;
			
			 public void addObject() { }

			 public Object borrowObject() throws Exception {
				 if (backend == null)
					 backend = new Backend(trackers, true);
				 
				 return backend;
			 }
			 
			 public void clear() { }
			 public void close() { }
			 public int getNumActive() { return 1; }
			 public int getNumIdle() { return 0; }
			 public void invalidateObject(Object obj) { 
				 backend = null;
			 }
			 
			 public void returnObject(Object obj) { }
			 public void setFactory(PoolableObjectFactory factory) { }
		};
	}

}
