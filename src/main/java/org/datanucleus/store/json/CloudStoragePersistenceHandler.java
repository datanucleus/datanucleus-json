/**********************************************************************
Copyright (c) 2010 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.json;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.Configuration;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.json.fieldmanager.FetchFieldManager;
import org.datanucleus.store.json.fieldmanager.StoreFieldManager;
import org.datanucleus.store.json.orgjson.JSONArray;
import org.datanucleus.store.json.orgjson.JSONException;
import org.datanucleus.store.json.orgjson.JSONObject;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public abstract class CloudStoragePersistenceHandler extends JsonPersistenceHandler
{
    public CloudStoragePersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    public void insertObject(DNStateManager sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (!storeMgr.managesClass(cmd.getFullClassName()))
        {
            // Make sure schema exists, using this connection
            storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), new String[] {cmd.getFullClassName()});
        }
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

        Map<String,String> options = new HashMap<String,String>();
        options.put(ConnectionFactoryImpl.STORE_JSON_URL, "/");
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(sm.getExecutionContext(), options);
        URLConnection conn = (URLConnection) mconn.getConnection();
        createBucket(conn, getHeaderForBucket());

        options.put(ConnectionFactoryImpl.STORE_JSON_URL, getURLPath(sm));
        options.put("Content-Type", "application/json");
        mconn = storeMgr.getConnectionManager().getConnection(sm.getExecutionContext(), options);
        conn = (URLConnection) mconn.getConnection();

        JSONObject jsonobj = new JSONObject();
        sm.provideFields(sm.getClassMetaData().getAllMemberPositions(), new StoreFieldManager(sm, jsonobj, true, table));
        write("PUT", conn.getURL().getPath(), conn, jsonobj.toString(), getHeaders("PUT",options));
    }

    protected void createBucket(URLConnection conn, Map headers)
    {
        //TODO this should be optional, based on a property
        try
        {
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE.debug("Creating bucket. ");
            }
            HttpURLConnection http = (HttpURLConnection)conn;

            Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<String, String> entry = iterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                http.setRequestProperty(key, value);
            }

            http.setRequestProperty("Content-Length", "0");
            http.setDoOutput(true);
            http.setRequestMethod("PUT");
            http.setReadTimeout(10000);
            http.setConnectTimeout(10000);
            http.connect();

            int code = http.getResponseCode();
            if (code == 409)
            {
                // HTTP Error code: 409 Conflict error: <?xml version='1.0' encoding='UTF-8'?><Error><Code>BucketAlreadyOwnedByYou</Code><Message>Your previous request to create the named bucket succeeded and you already own it.</Message></Error>
            }            
            else if (code >= 400)
            {
                StringBuilder sb = new StringBuilder();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int r;
                while ((r = http.getErrorStream().read(buffer)) != -1)
                {
                    baos.write(buffer, 0, r);
                }

                sb.append(new String(baos.toByteArray()));        
                http.getErrorStream().close();
                throw new NucleusDataStoreException("HTTP Error code: "+code+" "+http.getResponseMessage()+" error: "+sb.toString());
            }
            else if (code >= 300)
            {
                throw new NucleusDataStoreException("Redirect not supported. HTTP Error code: "+code+" "+http.getResponseMessage());
            }
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        }
    }
    
    protected Map<String, String> getHeaders(String httpVerb, Map<String,String> options)
    {
        Map<String, String> headers = super.getHeaders(httpVerb, options);
        String contentMD5 = "";
        String contentType = "";
        if (options.containsKey("Content-Type"))
        {
            contentType = options.get("Content-Type");
        }
        String urlStr = storeMgr.getConnectionURL();
        String authenticationKey = storeMgr.getConnectionUserName();
        String authenticationSecretKey = storeMgr.getConnectionPassword();

        try
        {
            urlStr = urlStr.substring(urlStr.indexOf(storeMgr.getStoreManagerKey()+":")+storeMgr.getStoreManagerKey().length()+1);
            headers.put("Host", getBucket()+"."+new URL(urlStr).getHost());
            String path = "/"+getBucket();
            if (options.containsKey(ConnectionFactoryImpl.STORE_JSON_URL))
            {
                if (!options.get(ConnectionFactoryImpl.STORE_JSON_URL).toString().startsWith("/"))
                {
                    path += "/";
                }
                if (options.get(ConnectionFactoryImpl.STORE_JSON_URL).toString().indexOf("?")>-1)
                {
                    path += options.get(ConnectionFactoryImpl.STORE_JSON_URL).toString().substring(0,options.get(ConnectionFactoryImpl.STORE_JSON_URL).toString().indexOf("?"));
                }
                else
                {
                    path += options.get(ConnectionFactoryImpl.STORE_JSON_URL).toString();
                }
            }
            else
            {
                path += new URL(urlStr).getPath();
            }
            String stringToSign = httpVerb + "\n" + contentMD5 + "\n" + contentType + "\n" + headers.get("Date") + "\n" + path;
            headers.put("Authorization", getRealmName()+" "+authenticationKey+":"+CloudStorageUtils.hmac(authenticationSecretKey,stringToSign));
        }
        catch (MalformedURLException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return headers;
    }
    
    protected abstract String getRealmName();
    
    protected Map getHeaderForBucket()
    {
        Map headers = new HashMap();
        headers.put("Date", CloudStorageUtils.getHTTPDate());
        String contentMD5 = "";
        String contentType = "";
        String httpVerb = "PUT";
        String urlStr = storeMgr.getConnectionURL();
        String awsKey = storeMgr.getConnectionUserName();
        String awsSecretKey = storeMgr.getConnectionPassword();
        try
        {
            urlStr = urlStr.substring(urlStr.indexOf(storeMgr.getStoreManagerKey()+":")+storeMgr.getStoreManagerKey().length()+1);
            headers.put("Host", getBucket()+"."+new URL(urlStr).getHost());
            String stringToSign = httpVerb + "\n" + contentMD5 + "\n" + contentType + "\n" + headers.get("Date") + "\n" + "/"+getBucket() +"/";
            headers.put("Authorization", "AWS "+awsKey+":"+CloudStorageUtils.hmac(awsSecretKey,stringToSign)); // TODO This is AWS specific, no?
        }
        catch (MalformedURLException e)
        {
            NucleusLogger.DATASTORE.error("Exception thrown getting header : ", e);
        }
        return headers;
    }    

    /**
     * Convenience method to get all objects of the candidate type from the specified connection.
     * @param ec ExecutionContext
     * @param mconn Managed Connection
     * @param candidateClass Candidate
     * @param subclasses Whether to include subclasses
     * @param ignoreCache Whether to ignore the cache
     * @return List of objects of the candidate type
     */
    public List getObjectsOfCandidateType(final ExecutionContext ec, ManagedConnection mconn, 
            Class candidateClass, boolean subclasses, boolean ignoreCache, Map options)
    {
        List results = new ArrayList();

        // TODO Support subclasses
        try
        {
            URLConnection conn = (URLConnection) mconn.getConnection();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            final AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

            JSONArray jsonarray;
            try
            {                
                HttpURLConnection http = (HttpURLConnection) conn;

                Iterator<Map.Entry<String, String>> iterator = getHeaders("GET", options).entrySet().iterator();
                while (iterator.hasNext())
                {
                    Map.Entry<String, String> entry = iterator.next();
                    String key = entry.getKey();
                    String value = entry.getValue();
                    http.setRequestProperty(key, value);
                }

                http.setDoInput(true);
                http.setRequestMethod("GET");
                http.setReadTimeout(10000);
                http.setConnectTimeout(10000);
                http.connect();
                int code = http.getResponseCode();
                if (code == 404)
                {
                    return Collections.EMPTY_LIST;
                }
                handleHTTPErrorCode(http);

                StringBuilder sb = new StringBuilder();
                if (http.getContentLength() > 0)
                {
                    for (int i = 0; i < http.getContentLength(); i++)
                    {
                        sb.append((char) http.getInputStream().read());
                    }
                }
                else
                {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[1024];
                    int r;
                    while ((r = http.getInputStream().read(buffer)) != -1)
                    {
                        baos.write(buffer, 0, r);
                    }
                    sb.append(new String(baos.toByteArray()));
                }
                http.getInputStream().close();

                String contentType = http.getHeaderField("content-type");
                //content-type = application/xml; charset=UTF-8  (charset is optional)
                if (contentType != null && 
                    (contentType.split(";")[0].equalsIgnoreCase("application/xml") || contentType.split(";")[0].equalsIgnoreCase("text/xml")) )
                {
                    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(sb.toString().getBytes()));
                    NodeList list = doc.getElementsByTagName("Contents");
                    Set set = new HashSet();
                    for (int i=0; i<list.getLength(); i++)
                    {
                        Element el = (Element) list.item(i);
                        JSONObject object = new JSONObject();
                        String keyText = el.getElementsByTagName("Key").item(0).getTextContent();
                        if (keyText.indexOf("/") < 1)
                        {
                            //log ignoring this element
                        }
                        else
                        {
                            String key = keyText.substring(keyText.indexOf("/",1)+1);
                            if (key.length()<1)
                            {
                                //log ignoring this element
                            }
                            else
                            {
                                String className = keyText.substring(0,keyText.indexOf("/"));
                                object.put("class", className);
                                object.put(cmd.getPrimaryKeyMemberNames()[0], key);
                                set.add(object);
                            }
                        }
                    }
                    jsonarray = new JSONArray(set);
                }
                else
                {
                    jsonarray = new JSONArray(sb.toString());
                }
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            catch (JSONException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            catch (SAXException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            catch (ParserConfigurationException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            for (int i = 0; i < jsonarray.length(); i++)
            {
                final JSONObject json = jsonarray.getJSONObject(i);

                Object id = null;
                final FieldManager fm = new FetchFieldManager(ec, cmd, json, table);
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    String memberName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
                    Object key = json.get(memberName);
                    if (key instanceof String)
                    {
                        id = ec.getNucleusContext().getIdentityManager().getDatastoreId((String)key);
                    }
                    else
                    {
                        id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), key);
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, true, fm);
                }

                Object pc = ec.findObject(id, new FieldValues()
                {
                    public FetchPlan getFetchPlanForLoading()
                    {
                        return null;
                    }
                    public void fetchNonLoadedFields(DNStateManager sm)
                    {
                        sm.replaceNonLoadedFields(cmd.getPKMemberPositions(), fm);
                    }
                    public void fetchFields(DNStateManager sm)
                    {
                        sm.replaceFields(cmd.getPKMemberPositions(), fm);
                    }
                }, null, ignoreCache, false);

                // Any fields loaded above will not be wrapped since we did not have StateManager at the point of creating the FetchFieldManager, so wrap them now
                ec.findStateManager(pc).replaceAllLoadedSCOFieldsWithWrappers();

                results.add(pc);
            }
        }
        catch (JSONException je)
        {
            // TODO Throw this
            NucleusLogger.DATASTORE.error("Exception thrown getting objects of type : ", je);
        }

        return results;
    }

    /**
     * URL path for querying in the cloud storage. it lists all entries for the bucket+a prefix
     */
    public String getURLPathForQuery(AbstractClassMetaData acmd)
    {
        String url = acmd.getValueForExtension("url");
        if (url == null)
        {
            url = acmd.getFullClassName();
        }        
        url = "?prefix="+url;
        return url;
    }
    
    private String getBucket()
    {
        Configuration conf = storeMgr.getNucleusContext().getConfiguration();
        return conf.getStringProperty("datanucleus.cloud.storage.bucket");
    }
}