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
package org.datanucleus.store.json.fieldmanager;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.json.CloudStorageUtils;
import org.datanucleus.store.json.orgjson.JSONException;
import org.datanucleus.store.json.orgjson.JSONObject;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager for inserting data into the provided JSONObject from the ObjectProvider.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    protected Table table;
    protected JSONObject jsonobj;

    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, JSONObject jsonobj, boolean insert, Table table)
    {
        super(ec, cmd, insert);
        this.jsonobj = jsonobj;
        this.table = table;
    }

    public StoreFieldManager(ObjectProvider op, JSONObject jsonobj, boolean insert, Table table)
    {
        super(op, insert);
        this.table = table;
        this.jsonobj = jsonobj;

        try
        {
            jsonobj.put("class", cmd.getFullClassName());
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), (boolean)value);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), (int)value);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), Character.valueOf(value));
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), (double)value);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), (double)value);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), (int)value);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), (long)value);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), (int)value);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        try
        {
            if (value == null)
            {
                jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), JSONObject.NULL);
            }
            else
            {
                jsonobj.put(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
            }
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
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
                else
                {
                    // Nested embedded object in JSON object
                    JSONObject embobj = new JSONObject();

                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                    embMmds.add(mmd);
                    ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embOP, embobj, insert, embMmds, table);
                    embOP.provideFields(embCmd.getAllMemberPositions(), storeEmbFM);
                    NucleusLogger.PERSISTENCE.warn("Member " + mmd.getFullFieldName() + " marked as embedded NESTED. This is experimental : " + embobj);

                    MemberColumnMapping mapping = getColumnMapping(fieldNumber); // TODO Update CompleteClassTable so that this has a mapping
                    String name = (mapping != null ? mapping.getColumn(0).getName() : mmd.getName());
                    jsonobj.put(name, embobj);
                    return;
                }
            }
            else
            {
                // Flat embedded. Store as multiple properties in the owner object
                int[] embMmdPosns = embCmd.getAllMemberPositions();
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);
                if (value == null)
                {
                    // Store null in all columns for the embedded (and nested embedded) object(s)
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, jsonobj, insert, embMmds, table);
                    for (int i=0;i<embMmdPosns.length;i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                        {
                            // Store a null for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            for (int j=0;j<mapping.getNumberOfColumns();j++)
                            {
                                jsonobj.put(mapping.getColumn(j).getName(), JSONObject.NULL);
                            }
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    return;
                }

                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embOP, jsonobj, insert, embMmds, table);
                embOP.provideFields(embMmdPosns, storeEmbFM);
                return;
            }
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // TODO Support nested embedding in JSON object
            throw new NucleusUserException("Dont support embedded multi-valued field at " + mmd.getFullFieldName() + " with Excel");
        }
    }
        
    protected void storeObjectFieldInternal(int fieldNumber, Object value, AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    throws JSONException
    {
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);
        String name = mapping.getColumn(0).getName();

        if (relationType == RelationType.NONE)
        {
            if (mapping.getTypeConverter() != null)
            {
                // Persist using the provided converter
                Object datastoreValue = mapping.getTypeConverter().toDatastoreType(value);
                if (mapping.getNumberOfColumns() > 1)
                {
                    if (value == null)
                    {
                        // TODO Cater for multiple columns
                        jsonobj.put(name, JSONObject.NULL);
                        return;
                    }

                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        // TODO Persist as the correct column type since the typeConverter type may not be directly persistable
                        Object colValue = Array.get(datastoreValue, i);
                        jsonobj.put(mapping.getColumn(i).getName(), colValue);
                    }
                }
                else
                {
                    if (value == null)
                    {
                        jsonobj.put(name, JSONObject.NULL);
                        return;
                    }

                    jsonobj.put(mapping.getColumn(0).getName(), datastoreValue);
                }
            }
            else
            {
                if (value == null)
                {
                    jsonobj.put(name, JSONObject.NULL);
                    return;
                }

                if (value instanceof Boolean)
                {
                    jsonobj.put(name, ((Boolean)value).booleanValue());
                }
                else if (value instanceof Integer)
                {
                    jsonobj.put(name, ((Integer)value).intValue());
                }
                else if (value instanceof Long)
                {
                    jsonobj.put(name, ((Long)value).longValue());
                }
                else if (value instanceof Double)
                {
                    jsonobj.put(name, ((Double)value).doubleValue());
                }
                else if (value instanceof Enum)
                {
                    if (MetaDataUtils.isJdbcTypeNumeric(mapping.getColumn(0).getJdbcType()))
                    {
                        jsonobj.put(name, ((Enum)value).ordinal());
                    }
                    else
                    {
                        jsonobj.put(name, ((Enum)value).name());
                    }
                }
                else if (value instanceof BigDecimal)
                {
                    jsonobj.put(name, value);
                }
                else if (value instanceof BigInteger)
                {
                    jsonobj.put(name, value);
                }
                else if (value instanceof Collection)
                {
                    // Collection<Non-PC> will be returned as JSONArray
                    jsonobj.put(name, (Collection)value);
                }
                else if (value instanceof Map)
                {
                    jsonobj.put(name, (Map)value);
                }
                // TODO Support array
                else
                {
                    // See if we can persist it as a Long/String
                    boolean useLong = MetaDataUtils.isJdbcTypeNumeric(mapping.getColumn(0).getJdbcType());
                    TypeConverter longConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
                    if (useLong)
                    {
                        if (longConv != null)
                        {
                            jsonobj.put(name, longConv.toDatastoreType(value));
                            return;
                        }
                    }
                    else
                    {
                        TypeConverter strConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                        if (strConv != null)
                        {
                            jsonobj.put(name, strConv.toDatastoreType(value));
                            return;
                        }
                        else if (longConv != null)
                        {
                            jsonobj.put(name, longConv.toDatastoreType(value));
                            return;
                        }
                    }

                    // Fallback to persist as a JSONObject and see what happens
                    JSONObject jsonobjfield = new JSONObject(value);
                    jsonobjfield.put("class", value.getClass().getName());
                    jsonobj.put(name, jsonobjfield);
                }

                return;
            }
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // 1-1, N-1 relation, so store the "id"
            if (value == null)
            {
                jsonobj.put(name, JSONObject.NULL);
                return;
            }

            Object valuePC = ec.persistObjectInternal(value, op, fieldNumber, -1);
            Object valueId = ec.getApiAdapter().getIdForObject(valuePC);
            jsonobj.put(name, IdentityUtils.getPersistableIdentityForId(valueId));
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            if (value == null)
            {
                jsonobj.put(name, JSONObject.NULL);
                return;
            }

            if (mmd.hasCollection())
            {
                Collection idColl = new ArrayList();
                Collection coll = (Collection)value;
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                    idColl.add(IdentityUtils.getPersistableIdentityForId(elementID));
                }
                jsonobj.put(name, idColl);
                return;
            }
            else if (mmd.hasArray())
            {
                Collection ids = new ArrayList(Array.getLength(value));
                for (int i=0;i<Array.getLength(value);i++)
                {
                    Object element = Array.get(value, i);
                    Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                    ids.add(IdentityUtils.getPersistableIdentityForId(elementID));
                }
                jsonobj.put(name, ids);
                return;
            }
            else if (mmd.hasMap())
            {
                AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
                AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());

                Map idMap = new HashMap();
                Map map = (Map)value;
                Iterator<Map.Entry> mapIter = map.entrySet().iterator();
                while (mapIter.hasNext())
                {
                    Map.Entry entry = mapIter.next();
                    Object key = null;
                    Object val = null;
                    if (keyCmd != null)
                    {
                        Object keyPC = ec.persistObjectInternal(entry.getKey(), op, fieldNumber, -1);
                        key = ec.getApiAdapter().getIdForObject(keyPC);
                        key = IdentityUtils.getPersistableIdentityForId(key);
                    }
                    else
                    {
                        key = entry.getKey();
                    }
                    if (valCmd != null)
                    {
                        Object valPC = ec.persistObjectInternal(entry.getValue(), op, fieldNumber, -1);
                        val = ec.getApiAdapter().getIdForObject(valPC);
                        val = IdentityUtils.getPersistableIdentityForId(val);
                    }
                    else
                    {
                        val = entry.getValue();
                    }
                    idMap.put(key, val);
                }
                jsonobj.put(name, idMap);
                return;
            }
        }

        throw new NucleusException("Dont currently support field " + mmd.getFullFieldName() + " of type " + mmd.getTypeName());
    }
}