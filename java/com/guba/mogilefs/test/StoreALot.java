package com.guba.mogilefs.test;

import java.io.File;

import org.apache.log4j.Logger;

import com.guba.mogilefs.LocalFileMogileFSImpl;
import com.guba.mogilefs.MogileFS;

public class StoreALot {
    
    private static Logger log = Logger.getLogger(StoreALot.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            // make a mogilefs object:
            MogileFS mfs = 
            
                new LocalFileMogileFSImpl(new File("."), "emloffice.guba.com"); 
            //  new PooledMogileFSImpl("emloffice.guba.com", new String[] { "hunter.guba.com:7001", "taylor.guba.com:7001" }, -1, 2, 10000);
        
            File file = new File(args[0]);
            int count = Integer.parseInt(args[1]);
            
            for (int i = 0; i < count; i++) {
                Thread thread = new Thread(new StoreSomething(mfs, file));
                thread.start();
            }
            

        } catch (Exception e) {
            log.error(e);
        }
    }
}
    
class StoreSomething implements Runnable {

    private static Logger log = Logger.getLogger(StoreSomething.class);

    private MogileFS mfs;
    private File file;
    
    public StoreSomething(MogileFS mfs, File file) {
        this.mfs = mfs;
        this.file = file;
    }
    
    public void run() {
        try {
            String key = Double.toString(Math.random());
            
            log.info("starting store of " + key);
            mfs.storeFile(key, "derived", file);
            log.info("ending store of " + key);
            
            log.info("starting delete of " + key);
            //mfs.delete(key);
            log.info("ending delete of " + key);
            
        } catch (Exception e) {
            log.error(e);
        }
    }
    
}
