/*
 * Created on Jun 27, 2005
 *
 * copyright ill.com 2005
 */
package com.guba.mogilefs.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;

import org.apache.log4j.BasicConfigurator;

import com.guba.mogilefs.PooledMogileFSImpl;
import com.guba.mogilefs.MogileFS;

/**
 * @author ericlambrecht
 *  
 */
public class TestMogileFS {

    //    private static Logger log = Logger.getLogger(TestMogileFS.class);

    public static void main(String[] args) {
        BasicConfigurator.configure();

        try {
            MogileFS mfs = new PooledMogileFSImpl("www.guba.com",
                    new String[] { "qbert.guba.com:7001" }, 0, 1, 10000);
            /**
             * // pull up a file String[] paths = mfs.getPaths("eric", true); if
             * (paths == null) { log.debug("didn't find file!"); } else { for
             * (int i = 0; i < paths.length; i++) { log.debug("found path " +
             * paths[0]); }
             * 
             * InputStream in = mfs.getFileData("eric"); BufferedReader reader =
             * new BufferedReader(new InputStreamReader(in)); String line; while
             * ((line = reader.readLine()) != null) { log.debug("got line " +
             * line); } reader.close(); }
             */

            File file = new File(
                    "/Users/ericlambrecht/Projects/mogilefs/java/com/guba/mogilefs/PooledMogileFSImpl.java");
            if (file.exists()) {
                OutputStream out = mfs.newFile("PooledMogileFSImpl.java",
                        "oneDeviceTest", file.length());
                FileInputStream in = new FileInputStream(file);
                byte[] buffer = new byte[1024];
                int count = 0;
                while ((count = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, count);
                }
                in.close();
                out.close();
            }

            //log.debug("success!");

        } catch (Exception e) {
            //log.error("top level exception", e);
        }
    }

}
