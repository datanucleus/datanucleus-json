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
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * FieldManager to handle the retrieval of information for an embedded persistable object from a JSON object.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public FetchEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, JSONObject jsonobj, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, cmd, jsonobj, table);
        this.mmds = mmds;
    }

    public FetchEmbeddedFieldManager(ObjectProvider op, JSONObject jsonobj, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(op, jsonobj, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    public Object fetchObjectField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this being a link back to the owner. TODO Repeat this for nested and their owners
            ObjectProvider[] ownerOps = op.getEmbeddedOwners();
            return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
        }

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            try
            {
                return fetchObjectFieldEmbedded(fieldNumber, mmd, clr, relationType);
            }
            catch (JSONException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }

        try
        {
            return fetchObjectFieldInternal(fieldNumber, mmd, clr, relationType);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    protected Object fetchObjectFieldEmbedded(int fieldNumber, AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    throws JSONException
    {
        if (RelationType.isRelationSingleValued(relationType))
        {
            // Can be stored nested in the JSON doc, or flat
            boolean nested = false;
            String nestedStr = mmd.getValueForExtension("nested");
            if (nestedStr != null && nestedStr.equalsIgnoreCase("true"))
            {
                nested = true;
            }

            AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
            embMmds.add(mmd);
            if (nested)
            {
                // Nested embedded object. JSONObject stored under this name
                MemberColumnMapping mapping = getColumnMapping(fieldNumber);
                String name = (mapping != null ? mapping.getColumn(0).getIdentifier() : mmd.getName());
                if (jsonobj.isNull(name))
                {
                    return null;
                }
                JSONObject embobj = jsonobj.getJSONObject(name);
                NucleusLogger.PERSISTENCE.warn("Member " + mmd.getFullFieldName() + " marked as embedded NESTED; This is experimental : " + embobj);

                ObjectProvider embOP = ec.newObjectProviderForEmbedded(embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, embobj, embMmds, table);
                embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embOP.getObject();
            }
            else
            {
                // Flat embedded. Stored as multiple properties in the owner object
                ObjectProvider embOP = ec.newObjectProviderForEmbedded(embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, jsonobj, embMmds, table);
                embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embOP.getObject();
            }
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // TODO Embedded Collection
            NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded container)");
            return null;
        }
        return null;
    }
}