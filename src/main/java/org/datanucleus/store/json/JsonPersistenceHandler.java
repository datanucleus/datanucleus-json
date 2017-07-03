/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URLConnection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
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
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

public class JsonPersistenceHandler extends AbstractPersistenceHandler
{
    JsonPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    public void close()
    {
        // nothing to do
    }

    public void insertObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        AbstractClassMetaData cmd = op.getClassMetaData();
        if (!storeMgr.managesClass(cmd.getFullClassName()))
        {
            // Make sure schema exists, using this connection
            storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
        }
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

        Map<String, String> options = new HashMap<String, String>();
        options.put(ConnectionFactoryImpl.STORE_JSON_URL, getURLPath(op));
        options.put("Content-Type", "application/json");

        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec, options);
        URLConnection conn = (URLConnection) mconn.getConnection();
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("JSON.Insert.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            JSONObject jsonobj = new JSONObject();
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String memberName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
                Object idKey = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
                try
                {
                    jsonobj.put(memberName, idKey);
                }
                catch (JSONException e)
                {
                    throw new NucleusException("Exception setting datastore identity in JSON object", e);
                }
            }

            if (cmd.isVersioned())
            {
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                String verColName = null;
                if (vermd.getFieldName() != null)
                {
                    verColName = table.getMemberColumnMappingForMember(cmd.getMetaDataForMember(vermd.getFieldName())).getColumn(0).getName();
                }
                else
                {
                    verColName = table.getSurrogateColumn(SurrogateColumnType.VERSION).getName(); // TODO Version stored in field?
                }

                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long versionNumber = 1;
                    op.setTransactionalVersion(Long.valueOf(versionNumber));
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(Localiser.msg("JSON.Insert.ObjectPersistedWithVersion", StringUtils.toJVMIDString(op.getObject()),
                            op.getInternalObjectId(), "" + versionNumber));
                    }
                    try
                    {
                        jsonobj.put(verColName, versionNumber);
                    }
                    catch (JSONException e)
                    {
                        throw new NucleusException("Exception setting version in JSON object", e);
                    }

                    if (vermd.getFieldName() != null)
                    {
                        // Version is stored in a field, so set it there too
                        AbstractMemberMetaData verfmd = cmd.getMetaDataForMember(vermd.getFieldName());
                        if (verfmd.getType() == Integer.class)
                        {
                            op.replaceField(verfmd.getAbsoluteFieldNumber(), Integer.valueOf((int) versionNumber));
                        }
                        else
                        {
                            op.replaceField(verfmd.getAbsoluteFieldNumber(), Long.valueOf(versionNumber));
                        }
                    }
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    Date date = new Date();
                    Timestamp ts = new Timestamp(date.getTime());
                    op.setTransactionalVersion(ts);
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(Localiser.msg("JSON.Insert.ObjectPersistedWithVersion", StringUtils.toJVMIDString(op.getObject()),
                            op.getInternalObjectId(), "" + ts));
                    }
                    try
                    {
                        jsonobj.put(verColName, ts.getTime());
                    }
                    catch (JSONException e)
                    {
                        throw new NucleusException("Exception setting version in JSON object", e);
                    }
                }
            }

            int[] fieldNumbers = cmd.getAllMemberPositions();
            op.provideFields(fieldNumbers, new StoreFieldManager(op, jsonobj, true, table));

            String jsonString = jsonobj.toString();
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("POST " + jsonString);
            }
            write("POST", conn.getURL().toExternalForm(), conn, jsonString, getHeaders("POST", options));

            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("JSON.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public void updateObject(ObjectProvider op, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        AbstractClassMetaData cmd = op.getClassMetaData();
        if (!storeMgr.managesClass(cmd.getFullClassName()))
        {
            // Make sure schema exists, using this connection
            storeMgr.manageClasses(ec.getClassLoaderResolver(), new String[]{cmd.getFullClassName()});
        }
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

        Map<String, String> options = new HashMap<String, String>();
        options.put(ConnectionFactoryImpl.STORE_JSON_URL, getURLPath(op));
        options.put("Content-Type", "application/json");

        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec, options);
        URLConnection conn = (URLConnection) mconn.getConnection();
        try
        {
            int[] updatedFieldNums = fieldNumbers;
            Object currentVersion = op.getTransactionalVersion();
            Object nextVersion = null;
            if (cmd.isVersioned())
            {
                // Version object so calculate version to store with
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Version field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    if (currentVersion instanceof Integer)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        currentVersion = Long.valueOf(((Integer) currentVersion).longValue());
                    }

                    nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                    if (verMmd.getType() == Integer.class || verMmd.getType() == int.class)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        nextVersion = Integer.valueOf(((Long) nextVersion).intValue());
                    }
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

                    boolean updatingVerField = false;
                    for (int i = 0; i < fieldNumbers.length; i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField)
                    {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length + 1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0, fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd.getAbsoluteFieldNumber();
                    }
                }
                else
                {
                    // Surrogate version column
                    nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                }
                op.setTransactionalVersion(nextVersion);
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuilder fieldStr = new StringBuilder();
                for (int i = 0; i < fieldNumbers.length; i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("JSON.Update.Start", op.getObjectAsPrintable(), op.getInternalObjectId(),
                    fieldStr.toString()));
            }

            JSONObject jsonobj = new JSONObject();
            if (cmd.isVersioned())
            {
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                String verColName = null;
                if (vermd.getFieldName() != null)
                {
                    verColName = table.getMemberColumnMappingForMember(cmd.getMetaDataForMember(vermd.getFieldName())).getColumn(0).getName();
                }
                else
                {
                    // Surrogate version
                    verColName = table.getSurrogateColumn(SurrogateColumnType.VERSION).getName();
                }

                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(Localiser.msg("JSON.Insert.ObjectPersistedWithVersion", StringUtils.toJVMIDString(op.getObject()),
                            op.getInternalObjectId(), "" + nextVersion));
                    }
                    try
                    {
                        jsonobj.put(verColName, nextVersion);
                    }
                    catch (JSONException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(Localiser.msg("JSON.Insert.ObjectPersistedWithVersion", StringUtils.toJVMIDString(op.getObject()),
                            op.getInternalObjectId(), "" + nextVersion));
                    }

                    Timestamp ts = (Timestamp) nextVersion;
                    Date date = new Date();
                    date.setTime(ts.getTime());
                    try
                    {
                        jsonobj.put(verColName, ts.getTime());
                    }
                    catch (JSONException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
            }

            FieldManager storeFM = new StoreFieldManager(op, jsonobj, false, table);
            op.provideFields(updatedFieldNums, storeFM);
            op.provideFields(op.getClassMetaData().getPKMemberPositions(), storeFM);

            String jsonString = jsonobj.toString();
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("PUT " + jsonString);
            }
            write("PUT", conn.getURL().toExternalForm(), conn, jsonString, getHeaders("PUT", options));

            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("JSON.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        Map<String, String> options = new HashMap<String, String>();
        options.put(ConnectionFactoryImpl.STORE_JSON_URL, getURLPath(op));
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec, options);
        URLConnection conn = (URLConnection) mconn.getConnection();
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("JSON.Delete.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            HttpURLConnection http = (HttpURLConnection) conn;
            Map<String, String> headers = getHeaders("DELETE", options);
            Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<String, String> entry = iterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                http.setRequestProperty(key, value);
            }
            http.setRequestMethod("DELETE");
            http.setReadTimeout(10000);
            http.setConnectTimeout(10000);
            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("DELETE " + op.getInternalObjectId());
            }
            http.connect();

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (http.getResponseCode() == 404)
            {
                throw new NucleusObjectNotFoundException();
            }
            handleHTTPErrorCode(http);

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("JSON.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void fetchObject(ObjectProvider op, int[] fieldNumbers)
    {
        Map<String, String> options = new HashMap<String, String>();
        options.put(ConnectionFactoryImpl.STORE_JSON_URL, getURLPath(op));
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec, options);
        URLConnection conn = (URLConnection) mconn.getConnection();
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(op.getObjectAsPrintable()).append("\" (id=");
                str.append(op.getInternalObjectId()).append(")").append(" fields [");
                for (int i = 0; i < fieldNumbers.length; i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                str.append("]");
                NucleusLogger.PERSISTENCE.debug(str.toString());
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("JSON.Fetch.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // Create JSON object with PK fields set and get the object
            JSONObject jsonobj = new JSONObject();
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String memberName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
                Object idKey = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
                try
                {
                    jsonobj.put(memberName, idKey);
                }
                catch (JSONException e)
                {
                    throw new NucleusException("Exception setting datastore identity in JSON object", e);
                }
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                op.provideFields(op.getClassMetaData().getPKMemberPositions(), new StoreFieldManager(op, jsonobj, true, table));
            }
            JSONObject result = read("GET", conn.getURL().toExternalForm(), conn, getHeaders("GET", options));

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("GET " + result.toString());
            }
            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
                ec.getStatistics().incrementFetchCount();
            }

            op.replaceFields(fieldNumbers, new FetchFieldManager(op, result, table));

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("JSON.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }

    public void locateObject(ObjectProvider op)
    {
        Map<String, String> options = new HashMap<String, String>();
        options.put(ConnectionFactoryImpl.STORE_JSON_URL, getURLPath(op));
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec, options);
        URLConnection conn = (URLConnection) mconn.getConnection();

        try
        {
            HttpURLConnection http = (HttpURLConnection) conn;
            Map<String, String> headers = getHeaders("HEAD", options);
            Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<String, String> entry = iterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                http.setRequestProperty(key, value);
            }
            http.setDoOutput(true);
            http.setRequestMethod("HEAD");
            http.setReadTimeout(10000);
            http.setConnectTimeout(10000);
            http.connect();
            int code = http.getResponseCode();

            if (ec.getStatistics() != null)
            {
                // Add to statistics
                ec.getStatistics().incrementNumReads();
            }

            if (code == 404)
            {
                throw new NucleusObjectNotFoundException();
            }
            handleHTTPErrorCode(http);
        }
        catch (IOException e)
        {
            throw new NucleusObjectNotFoundException(e.getMessage(), e);
        }
    }

    protected void write(String method, String requestUri, URLConnection conn, String jsonString, Map<String, String> headers)
    {
        try
        {
            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE.debug("Writing to URL " + requestUri + " content " + jsonString);
            }

            int length = jsonString.length();
            HttpURLConnection http = (HttpURLConnection) conn;
            Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<String, String> entry = iterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                http.setRequestProperty(key, value);
            }
            http.setRequestProperty("Content-Length", "" + length);
            http.setDoOutput(true);
            http.setRequestMethod(method);
            http.setReadTimeout(10000);
            http.setConnectTimeout(10000);
            http.connect();
            OutputStream os = conn.getOutputStream();
            os.write(jsonString.getBytes());
            os.flush();
            os.close();
            handleHTTPErrorCode(http);
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected JSONObject read(String method, String requestUri, URLConnection conn, Map headers)
    {
        try
        {
            HttpURLConnection http = (HttpURLConnection) conn;
            Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<String, String> entry = iterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                http.setRequestProperty(key, value);
            }
            // http.setDoOutput(true);
            http.setDoInput(true);
            http.setRequestMethod(method);
            http.setReadTimeout(10000);
            http.setConnectTimeout(10000);
            http.connect();

            int code = http.getResponseCode();
            if (code == 404)
            {
                throw new NucleusObjectNotFoundException();
            }
            /* String msg = */http.getResponseMessage();
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
            return new JSONObject(sb.toString());
        }
        catch (SocketTimeoutException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        catch (JSONException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    protected Map<String, String> getHeaders(String httpVerb, Map<String, String> options)
    {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Date", CloudStorageUtils.getHTTPDate());
        String contentType = "";
        if (options.containsKey("Content-Type"))
        {
            contentType = options.get("Content-Type");
            headers.put("Content-Type", contentType);
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
     * @param options Map of options
     * @return List of objects of the candidate type
     */
    public List getObjectsOfCandidateType(final ExecutionContext ec, ManagedConnection mconn, Class candidateClass, boolean subclasses, boolean ignoreCache,
            Map options)
    {
        List results = new ArrayList();

        // TODO Support subclasses
        try
        {
            URLConnection conn = (URLConnection) mconn.getConnection();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            final AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
            final Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

            JSONArray jsonarray;
            try
            {
                HttpURLConnection http = (HttpURLConnection) conn;
                Map headers = getHeaders("GET", options);
                Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator();
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
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("GET " + candidateClass.getName());
                }
                http.connect();

                if (ec.getStatistics() != null)
                {
                    // Add to statistics
                    ec.getStatistics().incrementNumReads();
                }

                int code = http.getResponseCode();
                if (code == 404)
                {
                    return Collections.EMPTY_LIST;
                }

                /* String msg = */http.getResponseMessage();
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
                jsonarray = new JSONArray(sb.toString());
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            catch (JSONException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            for (int i = 0; i < jsonarray.length(); i++)
            {
                final JSONObject json = jsonarray.getJSONObject(i);
                final FieldManager fetchFM = new FetchFieldManager(ec, cmd, json, table);
                Object id = null;
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    String memberName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
                    Object key = json.get(memberName);
                    if (key instanceof String)
                    {
                        id = ec.getNucleusContext().getIdentityManager().getDatastoreId((String) key);
                    }
                    else
                    {
                        id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), key);
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, true, fetchFM);
                }

                Object version = null;
                if (cmd.isVersioned())
                {
                    // Extract the version for applying to the object
                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    String verColName = null;
                    if (vermd.getFieldName() == null)
                    {
                        // Surrogate version
                        verColName = table.getSurrogateColumn(SurrogateColumnType.VERSION).getName();
                    }
                    else
                    {
                        verColName = table.getMemberColumnMappingForMember(cmd.getMetaDataForMember(vermd.getFieldName())).getColumn(0).getName();
                    }

                    long versionLong = -1;
                    try
                    {
                        versionLong = json.getLong(verColName);
                        if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                        {
                            version = versionLong;
                        }
                        else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                        {
                            version = new Timestamp(versionLong);
                        }
                    }
                    catch (JSONException e)
                    {
                        // ignore
                    }
                }

                Object obj = ec.findObject(id, new FieldValues()
                {
                    public FetchPlan getFetchPlanForLoading()
                    {
                        return null;
                    }

                    public void fetchNonLoadedFields(ObjectProvider op)
                    {
                        op.replaceNonLoadedFields(cmd.getAllMemberPositions(), fetchFM);
                    }

                    public void fetchFields(ObjectProvider op)
                    {
                        op.replaceFields(cmd.getAllMemberPositions(), fetchFM);
                    }
                }, null, ignoreCache, false);
                ObjectProvider op = ec.findObjectProvider(obj);

                if (cmd.isVersioned() && version != null)
                {
                    op.setVersion(version);
                }

                // Any fields loaded above will not be wrapped since we did not have the ObjectProvider at the point of creating the FetchFieldManager, so wrap them now
                op.replaceAllLoadedSCOFieldsWithWrappers();

                results.add(obj);
            }
        }
        catch (JSONException je)
        {
            throw new NucleusException(je.getMessage(), je);
        }

        return results;
    }

    protected String getURLPath(ObjectProvider op)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
        String url = getURLPath(cmd);

        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            url += IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Create JSON object with PK fields set and get the object
            JSONObject jsonobj = new JSONObject();
            op.provideFields(cmd.getPKMemberPositions(), new StoreFieldManager(op, jsonobj, true, table));
            try
            {
                // Append the PK to the URL
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[0]);
                String name = table.getMemberColumnMappingForMember(mmd).getColumn(0).getName();
                url += jsonobj.get(name).toString();
                // TODO Cater for multiple PK fields
            }
            catch (JSONException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }

        return url;
    }

    protected String getURLPath(AbstractClassMetaData acmd)
    {
        String url = acmd.getValueForExtension("url");
        if (url == null)
        {
            url = acmd.getFullClassName();
        }
        if (!url.endsWith("/"))
        {
            url += "/";
        }
        return url;
    }

    public String getURLPathForQuery(AbstractClassMetaData acmd)
    {
        String url = acmd.getValueForExtension("url");
        if (url == null)
        {
            url = acmd.getFullClassName();
        }
        if (!url.endsWith("/"))
        {
            url += "/";
        }
        return url;
    }

    protected void handleHTTPErrorCode(HttpURLConnection http) throws IOException
    {
        if (http.getResponseCode() >= 400)
        {
            StringBuilder sb = new StringBuilder();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int r;
            if (http.getErrorStream() != null)
            {
                while ((r = http.getErrorStream().read(buffer)) != -1)
                {
                    baos.write(buffer, 0, r);
                }
                sb.append(new String(baos.toByteArray()));
                http.getErrorStream().close();
            }

            throw new NucleusDataStoreException(
                    "Error on URL: '" + http.getURL().toExternalForm() + "' Request Method: " + http.getRequestMethod() + " HTTP Error code: " + 
                    http.getResponseCode() + " " + http.getResponseMessage() + " error: " + sb.toString());
        }
        else if (http.getResponseCode() >= 300)
        {
            throw new NucleusDataStoreException("Redirect not supported. HTTP Error code: " + http.getResponseCode() + " " + http.getResponseMessage());
        }
    }
}