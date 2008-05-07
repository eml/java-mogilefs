/*
 * Created on June 15, 2005
 *
 * 
 */
package com.guba.mogilefs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.pool.ObjectPool;
import org.apache.log4j.Logger;

/**
 * This class is the interface for storing something into the PooledMogileFSImpl store.
 * This differs from the original Perl in that I don't cache the whole damn
 * thing in memory, then spit it out when you close the stream. Instead, I just
 * open a connection to the storage node upon creation and start pumping things
 * directly to it. I'm guessing the server won't time out before we get all the
 * data to it.
 * 
 * @author eml
 */
public class MogileOutputStream extends OutputStream {

    private static Logger log = Logger.getLogger(MogileOutputStream.class);
    
    /**
     * Number of milliseconds we'll let this socket block before we consider it
     * timed out.
     */
    public static final int SOCKET_TIMEOUT = 60000;

    private ObjectPool backendPool;

    private String domain;

    private String fid;

    private String path;

    private String devid;

    private String key;

    private long totalBytes;

    private Socket socket;

    private OutputStream out;

    private BufferedReader reader;

    private int count;
    
    public MogileOutputStream(ObjectPool backendPool, String domain, String fid,
            String path, String devid, String key,
            long totalBytes) throws MalformedURLException,
            StorageCommunicationException {
        this.backendPool = backendPool;
        this.domain = domain;
        this.fid = fid;
        this.path = path;
        this.devid = devid;
        this.key = key;
        this.totalBytes = totalBytes;
        this.count = 0;

        try {
            // open a connection to the server
            socket = new Socket();
            socket.setSoTimeout(SOCKET_TIMEOUT);
            URL parsedPath = new URL(path);
            socket.connect(new InetSocketAddress(parsedPath.getHost(),
                    parsedPath.getPort()));
            out = socket.getOutputStream();
            reader = new BufferedReader(new InputStreamReader(socket
                    .getInputStream()));

            // let the server know what is coming
            Writer writer = new OutputStreamWriter(out);
            writer.write("PUT ");
            writer.write(parsedPath.getPath());
            writer.write(" HTTP/1.0\r\nContent-length: ");
            writer.write(Long.toString(totalBytes));
            writer.write("\r\n\r\n");
            writer.flush();
        } catch (IOException e) {
            // problem talking to the storage server
            throw new StorageCommunicationException(
                    "problem initiating communication with storage server before storing "
                            + path + ": " + e.getMessage(), e);
        }
    }

    public void close() throws IOException {
        if ((out == null) || (socket == null))
            throw new IOException("socket has been closed already");

        out.flush();

        String response = reader.readLine();
        if (response == null)
            throw new IOException("no response after putting file to "
                    + path.toString());

        Pattern validResponse = Pattern.compile("^HTTP/\\d+\\.\\d+\\s+(\\d+)");
        Matcher matcher = validResponse.matcher(response);

        if (!matcher.find()) {
            throw new IOException("response from put to " + path.toString()
                    + " not understood: " + response);
        }

        int responseCode = Integer.parseInt(matcher.group(1));
        if ((responseCode < 200) || (responseCode > 299)) {
            // we got an error - read through to the body
            StringBuffer fullResponse = new StringBuffer();
            fullResponse.append("Problem storing to ");
            fullResponse.append(path.toString());
            fullResponse.append("\n\n");
            fullResponse.append(response);
            fullResponse.append("\n");
            while ((response = reader.readLine()) != null) {
                fullResponse.append(response);
                fullResponse.append("\n");
            }

            throw new IOException(fullResponse.toString());
        }

        out.close();
        out = null;

        reader.close();
        reader = null;

        socket.close();
        socket = null;

        Backend backend = null;
        try {
            backend = borrowBackend();
            
            Map closeResponse = backend.doRequest("create_close", new String[] {
                    "fid", fid, "devid", devid, "domain", domain, "size",
                    Long.toString(totalBytes), "key", key, "path", path });

            if (closeResponse == null) {
                throw new IOException(backend.getLastErrStr());
            }
            
        } catch (IOException e) {
            // you know, I could throw this in the conditional above, but this
            // just seems clearer to me for some reason...
            if (backend != null) {
                invalidateBackend(backend);
                backend = null;
            }
            
            throw e;
            
        } catch (NoTrackersException e) {
            // I hate to not pass this on, but in the interest of keeping
            // this easily integrated with various clients, I'll wrap this
            // exception
            if (backend != null) {
                invalidateBackend(backend);
                backend = null;
            }
            
            throw new IOException(e.getMessage());

        } catch (TrackerCommunicationException e) {
            if (backend != null) {
                invalidateBackend(backend);
                backend = null;
            }
            
            throw new IOException(e.getMessage());
        
        } finally {
            if (backend != null)
                returnBackend(backend);
        }
    }

    public void flush() throws IOException {
        if ((out == null) || (socket == null))
            throw new IOException("socket has been closed already");

        out.flush();
    }

    public void write(int b) throws IOException {
        if ((out == null) || (socket == null))
            throw new IOException("socket has been closed already");

        try {
            count++;
            out.write(b);
        } catch (IOException e) {
            log.error("wrote at most " + count + "/" + totalBytes + " of stream to storage node " + socket.getInetAddress().getHostName());
            throw e;
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if ((out == null) || (socket == null))
            throw new IOException("socket has been closed already");

        try {
            count += len;
            out.write(b, off, len);
        } catch (IOException e) {
            log.error("wrote at most " + count + "/" + totalBytes + " of stream to storage node " + socket.getInetAddress().getHostName());
            throw e;
        }
    }

    public void write(byte[] b) throws IOException {
        if ((out == null) || (socket == null))
            throw new IOException("socket has been closed already");

        try {
            count += b.length;
            out.write(b);
        } catch (IOException e) {
            log.error("wrote at most " + count + "/" + totalBytes + " of stream to storage node " + socket.getInetAddress().getHostName());
            throw e;
        }
    }
    
    private Backend borrowBackend() throws NoTrackersException {
        try {
            return (Backend) backendPool.borrowObject();
            
        } catch (Exception e) {
            log.error(e);
            throw new NoTrackersException();
        }
    }
    
    private void returnBackend(Backend backend) {
        try {
            backendPool.returnObject(backend);
            
        } catch (Exception e) {
            // I think we can ignore this.
            log.warn(e);
        }
    }
    
    private void invalidateBackend(Backend backend) {
        try {
            backendPool.invalidateObject(backend);
            
        } catch (Exception e) {
            // I think we can ignore this
            log.warn(e);
        }
    }    
}
