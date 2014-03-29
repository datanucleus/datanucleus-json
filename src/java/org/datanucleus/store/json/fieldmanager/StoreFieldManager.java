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
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.converters.TypeConverter;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * FieldManager for inserting data into the provided JSONObject from the ObjectProvider.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    JSONObject jsonobj;
    StoreManager storeMgr;

    public StoreFieldManager(ObjectProvider op, JSONObject jsonobj, boolean insert)
    {
        super(op, insert);
        this.storeMgr = op.getExecutionContext().getStoreManager();
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

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, (boolean)value);
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, (int)value);
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, new Character(value));
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, (double)value);
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, (double)value);
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, (int)value);
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, (long)value);
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            jsonobj.put(name, (int)value);
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

        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
        try
        {
            if (value == null)
            {
                jsonobj.put(name, JSONObject.NULL);
            }
            else
            {
                jsonobj.put(name, value);
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
        if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
        {
            // Persistable object embedded into this table TODO Support this
            throw new NucleusException("Embedded fields are not supported");
        }

        try
        {
            storeObjectFieldInternal(fieldNumber, value, mmd, clr);
        }
        catch (JSONException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    protected void storeObjectFieldInternal(int fieldNumber, Object value, AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    throws JSONException
    {
        RelationType relationType = mmd.getRelationType(clr);
        String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);

        if (value == null)
        {
            jsonobj.put(name, JSONObject.NULL);
            return;
        }
        else if (relationType == RelationType.NONE)
        {
            if (mmd.getTypeConverterName() != null)
            {
                // User-defined converter
                TypeConverter conv = op.getExecutionContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
                jsonobj.put(name, conv.toDatastoreType(value));
                return;
            }
            else if (value instanceof Boolean)
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
                ColumnMetaData[] colmds = mmd.getColumnMetaData();
                boolean useNumeric = MetaDataUtils.persistColumnAsNumeric(colmds != null ? colmds[0] : null);
                if (useNumeric)
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
                jsonobj.put(name, value);
            }
            else if (value instanceof Map)
            {
                jsonobj.put(name, value);
            }
            else
            {
                // See if we can persist it as a Long/String
                boolean useLong = false;
                ColumnMetaData[] colmds = mmd.getColumnMetaData();
                if (colmds != null && colmds.length == 1 && MetaDataUtils.isJdbcTypeNumeric(colmds[0].getJdbcType()))
                {
                    useLong = true;
                }
                TypeConverter strConv = 
                    op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                TypeConverter longConv = 
                    op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
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
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // 1-1, N-1 relation, so store the "id"
            Object valuePC = op.getExecutionContext().persistObjectInternal(value, op, fieldNumber, -1);
            Object valueId = op.getExecutionContext().getApiAdapter().getIdForObject(valuePC);
            jsonobj.put(name, IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), valueId));
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            if (mmd.hasCollection())
            {
                Collection idColl = new ArrayList();
                Collection coll = (Collection)value;
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                    idColl.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), elementID));
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
                    Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                    ids.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), elementID));
                }
                jsonobj.put(name, ids);
                return;
            }
            else if (mmd.hasMap())
            {
                AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, op.getExecutionContext().getMetaDataManager());
                AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, op.getExecutionContext().getMetaDataManager());

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
                        key = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), key);
                    }
                    else
                    {
                        key = entry.getKey();
                    }
                    if (valCmd != null)
                    {
                        Object valPC = ec.persistObjectInternal(entry.getValue(), op, fieldNumber, -1);
                        val = ec.getApiAdapter().getIdForObject(valPC);
                        val = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), val);
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