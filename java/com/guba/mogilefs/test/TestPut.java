/*
 * Created on Oct 1, 2005
 *
 */
package com.guba.mogilefs.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;


/**
 * @author eml
 *
 */
public class TestPut {

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.println("usage: TestPut destination filename");
            System.exit(0);
        }
        
        String destination = args[0];
        String filename = args[1];
        
        try {
            // open a connection to the server
            Socket socket = new Socket();
            socket.setSoTimeout(0);
            URL parsedPath = new URL(destination);
            socket.connect(new InetSocketAddress(parsedPath.getHost(),
                    parsedPath.getPort()));
            OutputStream out = socket.getOutputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket
                    .getInputStream()));
            
            // get details of the file we're sending
            File file = new File(filename);
            FileInputStream in = new FileInputStream(file);

            // let the server know what is coming
            Writer writer = new OutputStreamWriter(out);
            writer.write("PUT ");
            writer.write(parsedPath.getPath());
            writer.write(" HTTP/1.0\r\nContent-length: ");
            writer.write(Long.toString(file.length()));
            writer.write("\r\n\r\n");
            writer.flush();
            
            // read in the file and write it out to the server
            byte[] bytes = new byte[1024];
            int count = 0;
            while ((count = in.read(bytes)) > 0) {
                out.write(bytes, 0, count);
            }
            
            in.close();
            //out.close();
            
            // done!
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                System.out.println("\n");
            }
            
            out.close();
            
        } catch (IOException e) {
            // problem talking to the storage server
            System.out.println("exception: "+ e.getMessage());
        }
    }
        
}
