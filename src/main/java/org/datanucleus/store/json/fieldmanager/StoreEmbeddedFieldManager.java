/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
 ...
***********************************************************************/
package org.datanucleus.store.json.fieldmanager;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.json.CloudStorageUtils;
import org.datanucleus.store.json.orgjson.JSONException;
import org.datanucleus.store.json.orgjson.JSONObject;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager to handle the store information for an embedded persistable object into JSON.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public StoreEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, JSONObject jsonobj, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, cmd, jsonobj, insert, table);
        this.mmds = mmds;
    }

    public StoreEmbeddedFieldManager(DNStateManager sm, JSONObject jsonobj, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(sm, jsonobj, insert, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData lastMmd = mmds.get(mmds.size()-1);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this member being a link back to the owner. TODO Repeat this for nested and their owners
            if (sm != null)
            {
                DNStateManager[] ownerSMs = ec.getOwnersForEmbeddedStateManager(sm);
                if (ownerSMs != null && ownerSMs.length == 1 && value != ownerSMs[0].getObject())
                {
                    // Make sure the owner field is set
                    sm.replaceField(fieldNumber, ownerSMs[0].getObject());
                }
            }
            return;
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, lastMmd))
        {
            // Embedded field
            try
            {
                storeObjectFieldEmbedded(fieldNumber, value, mmd, clr, relationType);
                return;
            }
            catch (JSONException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }

        if (sm == null)
        {
            // Null the column
            MemberColumnMapping mapping = getColumnMapping(fieldNumber);
            for (int i=0;i<mapping.getNumberOfColumns();i++)
            {
                // TODO Null the column(s)
            }
        }

        try
        {
            storeObjectFieldInternal(fieldNumber, value, mmd, clr, relationType);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    protected void storeObjectFieldEmbedded(int fieldNumber, Object value, AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    throws JSONException
    {
        if (RelationType.isRelationSingleValued(relationType))
        {
            // Embedded PC : Can be stored nested in the JSON doc, or flat
            boolean nested = CloudStorageUtils.isMemberNested(mmd);

            AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            if (nested)
            {
                // Nested embedded object. Store JSONObject under this name
                if (value == null)
                {
                    MemberColumnMapping mapping = getColumnMapping(fieldNumber);
                    String name = mapping.getColumn(0).getName();
                    jsonobj.put(name, JSONObject.NULL);
                    return;
                }

                // Nested embedded object in JSON object
                JSONObject embobj = new JSONObject();

                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);
                DNStateManager embSM = ec.findStateManagerForEmbedded(value, sm, mmd, null);
                StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embSM, embobj, insert, embMmds, table);
                embSM.provideFields(embCmd.getAllMemberPositions(), storeEmbFM);
                NucleusLogger.PERSISTENCE.warn("Member " + mmd.getFullFieldName() + " marked as embedded NESTED. This is experimental : " + embobj);

                MemberColumnMapping mapping = getColumnMapping(fieldNumber); // TODO Make sure CompleteClassTable has this mapping
                String name = (mapping != null ? mapping.getColumn(0).getName() : mmd.getName());
                jsonobj.put(name, embobj);
                return;
            }

            // Persistable object embedded into this table
            if (embCmd != null)
            {
                DNStateManager embSM = null;
                if (value != null)
                {
                    embSM = ec.findStateManagerForEmbedded(value, sm, mmd, null);
                }
                else
                {
                    embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embCmd, sm, fieldNumber, null);
                }

                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);
                embSM.provideFields(embCmd.getAllMemberPositions(), new StoreEmbeddedFieldManager(embSM, jsonobj, insert, embMmds, table));
                return;
            }
        }
        else
        {
            // TODO Embedded Collection
            NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded), storing as null");
            return;
        }
    }
}