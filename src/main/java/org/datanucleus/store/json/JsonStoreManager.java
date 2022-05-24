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
2008 Andy Jefferson - abstracted methods up to AbstractStoreManager
    ...
**********************************************************************/
package org.datanucleus.store.json;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.QueryLanguage;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.json.query.JDOQLQuery;
import org.datanucleus.store.json.query.JPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.Localiser;

/**
 * Store Manager for JSON datastores.
 */
public class JsonStoreManager extends AbstractStoreManager
{
    static
    {
        Localiser.registerBundle("org.datanucleus.store.json.Localisation", JsonStoreManager.class.getClassLoader());
    }

    public JsonStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super("json", clr, ctx, props);

        persistenceHandler = new JsonPersistenceHandler(this);
        connectionMgr.disableConnectionCaching();

        logConfiguration();
    }

    public NucleusConnection getNucleusConnection(ExecutionContext ec)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_APPLICATION_COMPOSITE_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC_NESTED);
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_DELETE);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_DELETE);
        set.add(StoreManager.OPTION_ORM_INHERITANCE_COMPLETE_TABLE);
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, java.lang.String)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec, queryString);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, org.datanucleus.store.query.Query)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q)
    {
        if (language.equals(QueryLanguage.JDOQL.name()))
        {
            return new JDOQLQuery(this, ec, (JDOQLQuery) q);
        }
        else if (language.equals(QueryLanguage.JPQL.name()))
        {
            return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isEmbeddedOnly())
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    // Make sure we have the appropriate "Table" object registered
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, null);
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }
                }
            }
        }
    }
}