/*
 * Created on Jun 15, 2005
 *
 */
package com.guba.mogilefs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.channels.IllegalBlockingModeException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * This class talks to the trackers. It randomly connects to one of them, and
 * tries to keep that connection around for future requests. Note that this is
 * most definitely _not_ thread safe.
 * 
 * @author eml
 * @based-on the Backend class in the Perl API
 */
class Backend {

    private static Logger log = Logger.getLogger(Backend.class);

    private List hosts;

    private Map<InetSocketAddress, Long> deadHosts;

    private String lastErr;

    private String lastErrStr;

    private SocketWithReaderAndWriter cachedSocket;

    private Pattern ERROR_PATTERN = Pattern.compile("^ERR\\s+(\\w+)\\s*(\\S*)");

    private static final int ERR_PART = 1;

    private static final int ERRSTR_PART = 2;

    private Pattern OK_PATTERN = Pattern.compile("^OK\\s+\\d*\\s*(\\S*)");

    private static final int ARGS_PART = 1;

    /**
     * Create the backend. Optionally connect to a tracker right now to ensure
     * one is available right off the bat.
     * 
     * @param hostStrings
     *            Array of hostnames of trackers
     * @param connect
     *            if true, try to connect to a socket
     * 
     * @throws NoTrackersException
     * @throws BadHostFormatException
     */

    public Backend(List trackers, boolean connectNow)
            throws NoTrackersException {
        reload(trackers, connectNow);
    }

    /**
     * Reset the list of trackers. Optionally try to connect to one of them
     * immediately.
     * 
     * @param hostStrings
     * @param connect
     * @throws NoTrackersException
     * @throws BadHostFormatException
     */

    public void reload(List trackers, boolean connectNow)
            throws NoTrackersException {
        this.hosts = trackers;

        if (hosts.size() == 0)
            throw new NoTrackersException();

        this.deadHosts = new HashMap<InetSocketAddress, Long>();

        this.lastErr = null;
        this.lastErrStr = null;

        cachedSocket = null;
        if (connectNow)
            cachedSocket = getSocket();
    }

 
    /**
     * Randomly pick from our list of hosts and try to connect to one of them.
     * If we get an error connecting to a host, then make a note that it is
     * dead. If we can't connect to any host, throw a 'NoTrackersException'.
     * This function never returns null.
     * 
     * @return
     */

    private SocketWithReaderAndWriter getSocket() throws NoTrackersException {
        int hostSize = hosts.size();
        int tries = (hostSize > 15) ? 15 : hostSize;
        int index = (int) Math.floor(hosts.size() * Math.random());

        long now = System.currentTimeMillis();
        while (tries-- > 0) {
            InetSocketAddress host = (InetSocketAddress) hosts.get(index++
                    % hostSize);

            // try dead hosts every 5 seconds
            Long deadTime = (Long) deadHosts.get(host);
            if ((deadTime != null) && (deadTime.longValue() > (now - 5000))) {
                if (log.isDebugEnabled()) {
                    log.debug(" skipping connect attempt to dead host " + host);
                }
                continue;
            }

            try {
                // connect to the server
                Socket socket = new Socket();
                // 30 second timeout
                socket.setSoTimeout(30000);
                socket.connect(host, 3000);

                if (log.isDebugEnabled()) {
                    log.debug("connected to tracker " + socket.getInetAddress().getHostName());
                }
                
                // if we made it here, then the connection is good!
                return new SocketWithReaderAndWriter(socket);

            } catch (IOException e) {
                log.warn("Unable to connect to tracker at " +
                 host.toString(), e);

            } catch (IllegalBlockingModeException e) {
                log.warn("Unable to connect to tracker at " +
                 host.toString(), e);

            } catch (IllegalArgumentException e) {
                log.warn("Unable to connect to tracker " + host.toString(),
                 e);

            }

            // something went wrong, so mark the host as dead
            log.warn("marking host " + host + " as dead");
            deadHosts.put(host, new Long(now));
        }

        // didn't find anything! throw an exception!
        throw new NoTrackersException();
    }

    /**
     * Send a request to the tracker. Call it like this: Map result =
     * doRequest("command", new String[] { "arg1", "value1", "arg2", "value2"
     * });
     * 
     * @throws NoTrackersException
     *             thrown if we can't get ahold of a tracker
     * @param command
     * @param args
     *            Optional arguments. May be null. This is a hash mapped to an
     *            array of strings.
     * @return null on error, otherwise results of command
     */

    public Map doRequest(String command, String[] args)
            throws NoTrackersException, TrackerCommunicationException {
        if ((command == null) || (args == null)) {
            log.error("null command or args sent to doRequest");
            return null;
        }

        String argString = encodeURLString(args);
        String request = command + " " + argString + "\r\n";

        if (log.isDebugEnabled()) {
            log.debug("command: "+ request);
        }
        
        if (cachedSocket != null) {
            // try our cached socket, but assume it might be bogus
            try {
                cachedSocket.getWriter().write(request);
                cachedSocket.getWriter().flush();

            } catch (IOException e) {
                log.debug("cached socket went bad while sending request");
                cachedSocket = null;
            }
        }

        if (cachedSocket == null) {
            // Either we don't have a cached socket, or the existing cached
            // socket
            // didn't work. Try to connect to another server.
            SocketWithReaderAndWriter socket = getSocket();

            try {
                socket.getWriter().write(request);
                socket.getWriter().flush();

            } catch (IOException e) {
                throw new TrackerCommunicationException(
                        "problem finding a working tracker in this list: "
                                + listKnownTrackers());

            }

            cachedSocket = socket;
        }

        try {
            // ok - we finally got a message off to a tracker
            // now get a response
            String response = cachedSocket.getReader().readLine();

            if (response == null)
                throw new TrackerCommunicationException(
                        "received null response from tracker at "
                                + cachedSocket.getSocket().getInetAddress());

            if (log.isDebugEnabled()) {
                log.debug("response: " + response);
            }
            
            Matcher ok = OK_PATTERN.matcher(response);
            if (ok.matches()) {
                // good response
                return decodeURLString(ok.group(ARGS_PART));
            }

            Matcher err = ERROR_PATTERN.matcher(response);
            if (err.matches()) {
                // error response
                lastErr = err.group(ERR_PART);
                lastErrStr = err.group(ERRSTR_PART);

                if (log.isDebugEnabled())
                    log.debug("error message from tracker: " + lastErr + ", " + lastErrStr);
                
                return null;
            }

            throw new TrackerCommunicationException(
                    "invalid server response from "
                            + cachedSocket.getSocket().getInetAddress() + ": "
                            + response);

        } catch (IOException e) {
            // problem reading the response
            log.warn("problem reading response from server (" +
             cachedSocket.getSocket().getInetAddress() + ")", e);

            throw new TrackerCommunicationException(
                    "problem talking to server at "
                            + cachedSocket.getSocket().getInetAddress(), e);
        }
    }

    /**
     * Return the last error code.
     * 
     * @return
     */

    public String getLastErr() {
        return lastErr;
    }

    /**
     * Return the last descriptive string associated with the last error
     * 
     * @return
     */

    public String getLastErrStr() {
        return lastErrStr;
    }

    /**
     * Make sexy string that lists all the trackers we know about.
     * 
     * @return
     */

    private String listKnownTrackers() {
        StringBuffer trackers = new StringBuffer();
        Iterator it = hosts.iterator();
        while (it.hasNext()) {
            InetSocketAddress host = (InetSocketAddress) it.next();

            if (trackers.length() > 0)
                trackers.append(", ");
            trackers.append(host.toString());
        }

        return trackers.toString();
    }

    /**
     * Encode a map of key, value pairs into a URL format
     * 
     * @param args
     * @return never returns null, unless java has a problem encoding UTF-8
     */

    private String encodeURLString(String[] args) {
        try {
            StringBuffer encoded = new StringBuffer();

            for (int i = 0; i < args.length; i += 2) {
                String key = args[i];
                String value = args[i + 1];

                if (encoded.length() > 0)
                    encoded.append("&");
                encoded.append(key);
                encoded.append("=");
                encoded.append(URLEncoder.encode(value, "UTF-8"));
            }

            return encoded.toString();

        } catch (UnsupportedEncodingException e) {
            log.error("problem encoding URL for tracker", e);

            // what to return here... this really shouldn't happen
            return null;
        }
    }

    /**
     * Decode a urlencoded string into a bunch of (key, value) pairs
     * 
     * @param encoded
     * @return only returns null if java has a problem decoding UTF-8, which
     *         should never happen
     */

    private Map<String, String> decodeURLString(String encoded) {
        HashMap<String, String> map = new HashMap<String, String>();
        try {
            if ((encoded == null) || (encoded.length() == 0))
                return map;

            String parts[] = encoded.split("&");
            for (int i = 0; i < parts.length; i++) {
                String pair[] = parts[i].split("=");

                if ((pair == null) || (pair.length != 2)) {
                    log.error("poorly encoded string: "+ encoded);
                    continue;
                }

                map.put(pair[0], URLDecoder.decode(pair[1], "UTF-8"));
            }

            return map;

        } catch (UnsupportedEncodingException e) {
            log.error("problem decoding URL from tracker", e);

            return null;
        }
    }
    
    /**
     * Retrieve the name of the tracker we're talking
     * to. Might return null.
     * 
     * @return
     */
    
    public String getTracker() {
        if (cachedSocket == null)
            return null;
        
        return cachedSocket.getTracker();
    }
    
    /**
     * Close any open connections we've got
     * 
     */
    
    public void destroy() {
        if (cachedSocket != null) {
            try {
                cachedSocket.getSocket().close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
    
    /**
     * Return true if we're connected to a remote backend
     */
    
    public boolean isConnected() {
    	return ((cachedSocket != null) && (cachedSocket.getSocket().isConnected()));
    }
}

/**
 * @author ericlambrecht
 *  
 */

class SocketWithReaderAndWriter {

    private Socket socket;

    private BufferedReader reader;

    private Writer writer;

    public SocketWithReaderAndWriter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket
                .getInputStream()));
        this.writer = new OutputStreamWriter(socket.getOutputStream());
    }

    /**
     * @return Returns the reader.
     */
    public BufferedReader getReader() {
        return reader;
    }

    /**
     * @return Returns the socket.
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * @return Returns the writer.
     */
    public Writer getWriter() {
        return writer;
    }

    /**
     * Make sure the socket is closed
     */
    
    public void close() {
    	if (socket != null) {
    		try {
    			socket.close();
    		} catch (IOException e) {
    			// ignore
    		}
    	}
    }
    
    /**
     * Make sure we close out any open connections
     * 
     */
    
    protected void finalize() {
    	close();
    }
    
    /**
     * Return the name of the tracker we're talking to
     * 
     * @return
     */
    public String getTracker() {
        return socket.getInetAddress().getHostName();
    }

}
