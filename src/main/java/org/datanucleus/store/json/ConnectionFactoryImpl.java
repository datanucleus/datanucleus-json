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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;

/**
 * Implementation of a ConnectionFactory for JSON. The connections are
 * only created and they are not managed. All operations besides getConnection are no-op.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    // TODO Where is this defined ? Persistence property ? If so then should not have "org."
    public static final String STORE_JSON_URL = "org.datanucleus.store.json.url";

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceName Name of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceName)
    {
        super(storeMgr, resourceName);
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction
     * associated to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param options Any options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        return new ManagedConnectionImpl(options);
    }

    /**
     * Implementation of a ManagedConnection for JSON.
     */
    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        Map options;

        public ManagedConnectionImpl(Map options)
        {
            this.options = options;
        }

        public void close()
        {
            super.close();
        }

        public Object getConnection()
        {
            String urlStr = storeMgr.getConnectionURL();
            urlStr = urlStr.substring(urlStr.indexOf(storeMgr.getStoreManagerKey()+":") +
                storeMgr.getStoreManagerKey().length()+1);
            if (options.containsKey(STORE_JSON_URL))
            {
                if(urlStr.endsWith("/") && options.get(STORE_JSON_URL).toString().startsWith("/"))
                {
                    urlStr += options.get(STORE_JSON_URL).toString().substring(1);
                }
                else if(!urlStr.endsWith("/") && !options.get(STORE_JSON_URL).toString().startsWith("/"))
                {
                    urlStr += "/"+options.get(STORE_JSON_URL).toString();
                }
                else
                {
                    urlStr += options.get(STORE_JSON_URL).toString();
                }
            }
            URL url;
            try
            {
                url = new URL(urlStr);
                return url.openConnection();
            }
            catch (MalformedURLException e)
            {
                throw new NucleusDataStoreException(e.getMessage(),e);
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(e.getMessage(),e);
            }
        }

        public XAResource getXAResource()
        {
            return null;
        }
    }
}