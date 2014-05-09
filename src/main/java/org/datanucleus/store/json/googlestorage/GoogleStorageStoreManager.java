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
package org.datanucleus.store.json.googlestorage;

import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;

public class GoogleStorageStoreManager extends AbstractStoreManager
{
    public GoogleStorageStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super("googlestorage", clr, ctx, props);

        // Handler for persistence process
        persistenceHandler = new GoogleStoragePersistenceHandler(this);
        connectionMgr.disableConnectionPool();

        // See NUCJSON-16
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true"); 

        logConfiguration();
    }

    public NucleusConnection getNucleusConnection(ExecutionContext ec)
    {
        throw new UnsupportedOperationException();
    }
}