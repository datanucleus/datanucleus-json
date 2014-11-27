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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.json.CloudStorageUtils;
import org.datanucleus.store.json.orgjson.JSONArray;
import org.datanucleus.store.json.orgjson.JSONException;
import org.datanucleus.store.json.orgjson.JSONObject;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.TypeConversionHelper;

/**
 * FieldManager for fetching from JSON.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected final Table table;
    protected final JSONObject jsonobj;
    protected final StoreManager storeMgr;

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, JSONObject jsonobj, Table table)
    {
        super(ec, cmd);
        this.jsonobj = jsonobj;
        this.storeMgr = ec.getStoreManager();
        this.table = table;
    }

    public FetchFieldManager(ObjectProvider op, JSONObject jsonobj, Table table)
    {
        super(op);
        this.jsonobj = jsonobj;
        this.storeMgr = ec.getStoreManager();
        this.table = table;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return false;
        }
        try
        {
            return jsonobj.getBoolean(memberName);
        }
        catch (JSONException e)
        {
            //ignore
            return false;
        }
    }

    public byte fetchByteField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return 0;
        }
        try
        {
            String str = jsonobj.getString(memberName);
            return Byte.valueOf(str).byteValue();
        }
        catch (JSONException e)
        {
            //ignore
            return 0;
        }
    }

    public char fetchCharField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return 0;
        }
        try
        {
            return jsonobj.getString(memberName).charAt(0);
        }
        catch (JSONException e)
        {
            //ignore
            return 0;
        }
    }

    public double fetchDoubleField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return 0;
        }
        try
        {
            return jsonobj.getDouble(memberName);
        }
        catch (JSONException e)
        {
            //ignore
            return 0;
        }
    }

    public float fetchFloatField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return 0;
        }
        try
        {
            return (float) jsonobj.getDouble(memberName);
        }
        catch (JSONException e)
        {
            //ignore
            return 0;
        }
    }

    public int fetchIntField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return 0;
        }
        try
        {
            return jsonobj.getInt(memberName);
        }
        catch (JSONException e)
        {
            //ignore
            return 0;
        }
    }
    
    public long fetchLongField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return 0;
        }
        try
        {
            return jsonobj.getLong(memberName);
        }
        catch (JSONException e)
        {
            //ignore
            return 0;
        }
    }

    public short fetchShortField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return 0;
        }
        try
        {
            return (short) jsonobj.getInt(memberName);
        }
        catch (JSONException e)
        {
            //ignore
            return 0;
        }
    }

    public String fetchStringField(int fieldNumber)
    {
        String memberName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (jsonobj.isNull(memberName))
        {
            return null;
        }
        try
        {
            return jsonobj.getString(memberName);
        }
        catch (JSONException e)
        {
            //ignore
            return null;
        }
    }

    public Object fetchObjectField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
        {
            return op.provideField(fieldNumber);
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
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
        // Embedded field
        if (RelationType.isRelationSingleValued(relationType))
        {
            // Can be stored nested in the JSON doc, or flat
            boolean nested = CloudStorageUtils.isMemberNested(mmd);

            AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
            embMmds.add(mmd);
            if (nested)
            {
                // Nested embedded object. JSONObject stored under this name
                MemberColumnMapping mapping = getColumnMapping(fieldNumber);
                String name = (mapping != null ? mapping.getColumn(0).getName() : mmd.getName());
                if (jsonobj.isNull(name))
                {
                    return null;
                }
                JSONObject embobj = jsonobj.getJSONObject(name);
                NucleusLogger.PERSISTENCE.warn("Member " + mmd.getFullFieldName() + " marked as embedded NESTED; This is experimental : " + embobj);

                ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, embobj, embMmds, table);
                embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embOP.getObject();
            }

            // Flat embedded. Stored as multiple properties in the owner object
            // TODO Null detection
            ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embCmd, op, fieldNumber);
            FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, jsonobj, embMmds, table);
            embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
            return embOP.getObject();
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // TODO Support nested embedding in JSON object
            throw new NucleusUserException("Dont support embedded multi-valued field at " + mmd.getFullFieldName() + " with Excel");
        }
        return null;
    }

    protected Object fetchObjectFieldInternal(int fieldNumber, AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    throws JSONException
    {
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);
        if (relationType == RelationType.NONE)
        {
            Object returnValue = null;
            if (mapping.getTypeConverter() != null)
            {
                TypeConverter conv = ec.getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
                if (mapping.getNumberOfColumns() > 1)
                {
                    boolean isNull = true;
                    Object valuesArr = null;
                    Class[] colTypes = ((MultiColumnConverter)conv).getDatastoreColumnTypes();
                    if (colTypes[0] == int.class)
                    {
                        valuesArr = new int[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == long.class)
                    {
                        valuesArr = new long[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == double.class)
                    {
                        valuesArr = new double[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == float.class)
                    {
                        valuesArr = new double[mapping.getNumberOfColumns()];
                    }
                    else if (colTypes[0] == String.class)
                    {
                        valuesArr = new String[mapping.getNumberOfColumns()];
                    }
                    // TODO Support other types
                    else
                    {
                        valuesArr = new Object[mapping.getNumberOfColumns()];
                    }

                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        String colName = mapping.getColumn(i).getName();
                        if (colTypes[i] == String.class)
                        {
                            Array.set(valuesArr, i, jsonobj.getString(colName));
                        }
                        else if (colTypes[i] == Boolean.class)
                        {
                            Array.set(valuesArr, i, Boolean.valueOf(jsonobj.getBoolean(colName)));
                        }
                        else if (colTypes[i] == Double.class)
                        {
                            Array.set(valuesArr, i, Double.valueOf(jsonobj.getDouble(colName)));
                        }
                        else if (colTypes[i] == Float.class)
                        {
                            Array.set(valuesArr, i, Float.valueOf((float)jsonobj.getDouble(colName)));
                        }
                        else if (colTypes[i] == Integer.class)
                        {
                            Array.set(valuesArr, i, Integer.valueOf(jsonobj.getInt(colName)));
                        }
                        else if (colTypes[i] == Long.class)
                        {
                            Array.set(valuesArr, i, Long.valueOf(jsonobj.getLong(colName)));
                        }
                        else if (colTypes[i] == double.class)
                        {
                            Array.set(valuesArr, i, jsonobj.getDouble(colName));
                        }
                        else if (colTypes[i] == float.class)
                        {
                            Array.set(valuesArr, i, jsonobj.getDouble(colName));
                        }
                        else if (colTypes[i] == int.class)
                        {
                            Array.set(valuesArr, i, jsonobj.getInt(colName));
                        }
                        else if (colTypes[i] == long.class)
                        {
                            Array.set(valuesArr, i, jsonobj.getLong(colName));
                        }
                        // TODO Support other types
                    }

                    if (isNull)
                    {
                        return null;
                    }

                    Object memberValue = conv.toMemberType(valuesArr);
                    if (op != null && memberValue != null)
                    {
                        memberValue = SCOUtils.wrapSCOField(op, fieldNumber, memberValue, true);
                    }
                    return memberValue;
                }

                String colName = mapping.getColumn(0).getName();
                if (jsonobj.isNull(colName))
                {
                    return null;
                }

                Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(conv, mmd.getType());
                if (datastoreType == String.class)
                {
                    returnValue = conv.toMemberType(jsonobj.getString(colName));
                }
                else if (datastoreType == Boolean.class)
                {
                    returnValue = conv.toMemberType(jsonobj.getBoolean(colName));
                }
                else if (datastoreType == Double.class)
                {
                    returnValue = conv.toMemberType(jsonobj.getDouble(colName));
                }
                else if (datastoreType == Float.class)
                {
                    returnValue = conv.toMemberType(jsonobj.getDouble(colName));
                }
                else if (datastoreType == Integer.class)
                {
                    returnValue = conv.toMemberType(jsonobj.getInt(colName));
                }
                else if (datastoreType == Long.class)
                {
                    returnValue = conv.toMemberType(jsonobj.getLong(colName));
                }
                // TODO Support Date types persisted using converter
                if (op != null)
                {
                    returnValue = SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true);
                }
                return returnValue;
            }

            String colName = mapping.getColumn(0).getName();
            if (jsonobj.isNull(colName))
            {
                return null;
            }

            if (Boolean.class.isAssignableFrom(mmd.getType()))
            {
                return jsonobj.getBoolean(colName);
            }
            else if (Integer.class.isAssignableFrom(mmd.getType()))
            {
                return jsonobj.getInt(colName);
            }
            else if (Long.class.isAssignableFrom(mmd.getType()))
            {
                return jsonobj.getLong(colName);
            }
            else if (Double.class.isAssignableFrom(mmd.getType()))
            {
                return jsonobj.getDouble(colName);
            }
            else if (Enum.class.isAssignableFrom(mmd.getType()))
            {
                if (MetaDataUtils.isJdbcTypeNumeric(mapping.getColumn(0).getJdbcType()))
                {
                    return mmd.getType().getEnumConstants()[jsonobj.getInt(colName)];
                }

                return Enum.valueOf(mmd.getType(), (String)jsonobj.get(colName));
            }
            else if (BigDecimal.class.isAssignableFrom(mmd.getType()) || BigInteger.class.isAssignableFrom(mmd.getType()))
            {
                return TypeConversionHelper.convertTo(jsonobj.get(colName), mmd.getType());
            }
            else if (Collection.class.isAssignableFrom(mmd.getType()))
            {
                // Collection<Non-PC>
                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                JSONArray array = jsonobj.getJSONArray(colName);
                Class elementCls = null;
                if (mmd.getCollection() != null && mmd.getCollection().getElementType() != null)
                {
                    elementCls = clr.classForName(mmd.getCollection().getElementType());
                }
                for (int i=0; i<array.length(); i++)
                {
                    if (array.isNull(i))
                    {
                        coll.add(null);
                    }
                    else
                    {
                        Object value = array.get(i);
                        if (value instanceof JSONObject)
                        {
                            Class cls = clr.classForName(((JSONObject)value).getString("class"), true);
                            coll.add(getNonpersistableObjectFromJSON((JSONObject)value, cls, clr));
                        }
                        else
                        {
                            if (elementCls != null)
                            {
                                coll.add(TypeConversionHelper.convertTo(value, elementCls));
                            }
                            else
                            {
                                coll.add(value);
                            }
                        }
                    }
                }

                if (op != null)
                {
                    SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), coll, true);
                }
                return coll;
            }
            else if (Map.class.isAssignableFrom(mmd.getType()))
            {
                // Map<Non-PC, Non-PC>
                Map map;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                    map = (Map) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                JSONObject mapValue = jsonobj.getJSONObject(colName);
                Iterator keyIter = mapValue.keys();
                Class keyCls = null;
                if (mmd.getMap() != null && mmd.getMap().getKeyType() != null)
                {
                    keyCls = clr.classForName(mmd.getMap().getKeyType());
                }
                Class valCls = null;
                if (mmd.getMap() != null && mmd.getMap().getValueType() != null)
                {
                    valCls = clr.classForName(mmd.getMap().getValueType());
                }

                while (keyIter.hasNext())
                {
                    Object jsonKey = keyIter.next();

                    Object key = jsonKey;
                    if (keyCls != null)
                    {
                        key = TypeConversionHelper.convertTo(jsonKey, keyCls);
                    }

                    Object jsonVal = mapValue.get((String)key);
                    Object val = jsonVal;
                    if (jsonVal instanceof JSONObject)
                    {
                        Class cls = clr.classForName(((JSONObject)jsonVal).getString("class"), true);
                        val = getNonpersistableObjectFromJSON((JSONObject)jsonVal, cls, clr);
                    }
                    else
                    {
                        if (valCls != null)
                        {
                            val = TypeConversionHelper.convertTo(jsonVal, valCls);
                        }
                    }
                    map.put(key, val);
                }

                if (op != null)
                {
                    SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), map, true);
                }
                return map;
            }
            else if (mmd.getType().isArray())
            {
                // Non-PC[]
                JSONArray arrayJson = jsonobj.getJSONArray(colName);
                Object array = Array.newInstance(mmd.getType().getComponentType(), arrayJson.length());
                for (int i=0; i<arrayJson.length(); i++)
                {
                    if (arrayJson.isNull(i))
                    {
                        Array.set(array, i, null);
                    }
                    else
                    {
                        Object value = arrayJson.get(i);
                        if (value instanceof JSONObject)
                        {
                            JSONObject valueJson = (JSONObject)value;
                            Class valueCls = clr.classForName(valueJson.getString("class"));
                            Array.set(array, i, getNonpersistableObjectFromJSON((JSONObject)value, valueCls, clr));
                        }
                        else
                        {
                            Array.set(array, i, TypeConversionHelper.convertTo(value, mmd.getType().getComponentType()));
                        }
                    }
                }
                return array;
            }
            else
            {
                // Fallback to built-in type converters
                boolean useLong = MetaDataUtils.isJdbcTypeNumeric(mapping.getColumn(0).getJdbcType());
                TypeConverter strConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                TypeConverter longConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);

                if (useLong && longConv != null)
                {
                    returnValue = longConv.toMemberType(jsonobj.getLong(colName));
                }
                else if (!useLong && strConv != null)
                {
                    returnValue = strConv.toMemberType(jsonobj.get(colName));
                }
                else if (!useLong && longConv != null)
                {
                    returnValue = longConv.toMemberType(jsonobj.getLong(colName));
                }
                else
                {
                    Object value = jsonobj.get(colName);
                    if (value instanceof JSONObject)
                    {
                        Class cls = clr.classForName(((JSONObject)value).getString("class"), true);
                        returnValue = getNonpersistableObjectFromJSON((JSONObject)value, cls, clr);
                    }
                    else
                    {
                        returnValue = TypeConversionHelper.convertTo(jsonobj.get(colName), mmd.getType());
                    }
                }

                if (op != null)
                {
                    SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true);
                }
                return returnValue;
            }
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object - retrieve the string form of the identity, and find the object
            String colName = mapping.getColumn(0).getName();
            if (jsonobj.isNull(colName))
            {
                return null;
            }

            String idStr = (String)jsonobj.get(colName);
            Object obj = null;
            AbstractClassMetaData memberCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            if (memberCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
            {
                // Uses persistent identity
                obj = IdentityUtils.getObjectFromPersistableIdentity(idStr, memberCmd, ec);
            }
            else
            {
                // Uses legacy identity
                obj = IdentityUtils.getObjectFromIdString(idStr, memberCmd, ec, true);
            }
            return obj;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            String colName = mapping.getColumn(0).getName();
            if (jsonobj.isNull(colName))
            {
                return null;
            }

            if (mmd.hasCollection())
            {
                // Collection<PC>
                JSONArray array = (JSONArray)jsonobj.get(colName);
                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(
                    ec.getClassLoaderResolver(), ec.getMetaDataManager());
                for (int i=0;i<array.length();i++)
                {
                    String idStr = (String)array.get(i);
                    Object element = null;
                    if (elementCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
                    {
                        // Uses persistent identity
                        element = IdentityUtils.getObjectFromPersistableIdentity(idStr, elementCmd, ec);
                    }
                    else
                    {
                        // Uses legacy identity
                        element = IdentityUtils.getObjectFromIdString(idStr, elementCmd, ec, true);
                    }
                    coll.add(element);
                }

                if (op != null)
                {
                    return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), coll, true);
                }
                return coll;
            }
            else if (mmd.hasArray())
            {
                // PC[]
                JSONArray array = (JSONArray)jsonobj.get(colName);
                Object arrayField = Array.newInstance(mmd.getType().getComponentType(), array.length());

                AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(
                    ec.getClassLoaderResolver(), ec.getMetaDataManager());
                for (int i=0;i<array.length();i++)
                {
                    String idStr = (String)array.get(i);
                    Object element = null;
                    if (elementCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
                    {
                        // Uses persistent identity
                        element = IdentityUtils.getObjectFromPersistableIdentity(idStr, elementCmd, ec);
                    }
                    else
                    {
                        // Uses legacy identity
                        element = IdentityUtils.getObjectFromIdString(idStr, elementCmd, ec, true);
                    }
                    Array.set(arrayField, i, element);
                }

                if (op != null)
                {
                    return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), arrayField, true);
                }
                return arrayField;
            }
            else if (mmd.hasMap())
            {
                // Map<Non-PC, PC>, Map<PC, PC>, Map<PC, Non-PC>
                JSONObject mapVal = (JSONObject)jsonobj.get(colName);
                Map map;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                    map = (Map) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
                AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());

                Iterator keyIter = mapVal.keys();
                while (keyIter.hasNext())
                {
                    Object jsonKey = keyIter.next();
                    Object key = null;
                    if (keyCmd != null)
                    {
                        // The jsonKey is the string form of the identity
                        String idStr = (String)jsonKey;
                        if (keyCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
                        {
                            // Uses persistent identity
                            key = IdentityUtils.getObjectFromPersistableIdentity(idStr, keyCmd, ec);
                        }
                        else
                        {
                            // Uses legacy identity
                            key = IdentityUtils.getObjectFromIdString(idStr, keyCmd, ec, true);
                        }
                    }
                    else
                    {
                        Class keyCls = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
                        key = TypeConversionHelper.convertTo(jsonKey, keyCls);
                    }

                    Object jsonVal = mapVal.get((String)key);
                    Object val = null;
                    if (valCmd != null)
                    {
                        // The jsonVal is the string form of the identity
                        String idStr = (String)jsonVal;
                        if (valCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
                        {
                            // Uses persistent identity
                            val = IdentityUtils.getObjectFromPersistableIdentity(idStr, valCmd, ec);
                        }
                        else
                        {
                            // Uses legacy identity
                            val = IdentityUtils.getObjectFromIdString(idStr, valCmd, ec, true);
                        }
                    }
                    else
                    {
                        Class valCls = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
                        val = TypeConversionHelper.convertTo(jsonVal, valCls);
                    }

                    map.put(key, val);
                }

                if (op != null)
                {
                    return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), map, true);
                }
                return map;
            }
        }

        throw new NucleusException("Dont currently support field " + mmd.getFullFieldName() + " of type " + mmd.getTypeName());
    }

    /**
     * Deserialise from JSON to a non-persistable object.
     * @param jsonobj JSONObject
     * @param cls The class of the object required
     * @param clr ClassLoader resolver
     * @return The object
     */
    private Object getNonpersistableObjectFromJSON(final JSONObject jsonobj, final Class cls, final ClassLoaderResolver clr)
    {
        if (cls.getName().equals("com.google.appengine.api.users.User"))
        {
            return getComGoogleAppengineApiUsersUserFromJSON(jsonobj, cls, clr);
        }
        else if (cls.getName().equals("com.google.appengine.api.datastore.Key"))
        {
            return getComGoogleAppengineApiDatastoreKeyFromJSON(jsonobj, cls, clr);
        }
        else
        {
            // Try to reconstruct the object as a Java bean
            try
            {
                return AccessController.doPrivileged(new PrivilegedAction()
                {
                    public Object run()
                    {
                        try
                        {
                            Constructor c = ClassUtils.getConstructorWithArguments(cls, new Class[]{});
                            c.setAccessible(true);
                            Object obj = c.newInstance(new Object[]{});
                            String[] fieldNames = JSONObject.getNames(jsonobj);
                            for (int i = 0; i < jsonobj.length(); i++)
                            {
                                //ignore class field
                                if (!fieldNames[i].equals("class"))
                                {
                                    Field field = cls.getField(fieldNames[i]);
                                    field.setAccessible(true);
                                    field.set(obj, jsonobj.get(fieldNames[i]));
                                }
                            }
                            return obj;
                        }
                        catch (Exception e)
                        {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return null;
                    }
                });
            }
            catch (SecurityException ex)
            {
                ex.printStackTrace();
            }

        }
        return null;
    }

    /**
     * Convenience method to create an AppEngine User from a JSON object.
     * TODO Move this out somewhere else
     * @param jsonobj The JSONObject
     * @param cls Class being represented (User)
     * @param clr ClassLoader resolver
     * @return The Key
     */
    protected Object getComGoogleAppengineApiUsersUserFromJSON(JSONObject jsonobj, Class cls, ClassLoaderResolver clr)
    {
        String email = null;
        String authDomain = null;
        try
        {
            email = jsonobj.getString("email");
        }
        catch (JSONException e)
        {
            // should not happen if the field exists
        }
        try
        {
            authDomain = jsonobj.getString("authDomain");
        }
        catch (JSONException e)
        {
            // should not happen if the field exists
        }
        return ClassUtils.newInstance(cls, new Class[]{String.class, String.class}, new String[]{email, authDomain});
    }

    /**
     * Convenience method to create an AppEngine Key from a JSON object.
     * TODO Move this out somewhere else
     * @param jsonobj The JSONObject
     * @param cls Class being represented (Key)
     * @param clr ClassLoader resolver
     * @return The Key
     */
    protected Object getComGoogleAppengineApiDatastoreKeyFromJSON(JSONObject jsonobj, Class cls, ClassLoaderResolver clr)
    {
        try
        {
            Object parent = null;
            if (jsonobj.has("parent") && !jsonobj.isNull("parent"))
            {
                // if it's a JSONObject
                JSONObject parentobj = jsonobj.getJSONObject("parent");
                parent = getNonpersistableObjectFromJSON(parentobj, clr.classForName(jsonobj.getString("class")), clr);
            }

            if (jsonobj.has("appId"))
            {
                String appId = jsonobj.getString("appId");
                String kind = jsonobj.getString("kind");
                Class keyFactory = clr.classForName("com.google.appengine.api.datastore.KeyFactory",
                    cls.getClassLoader(), false);
                if (parent != null)
                {
                    return ClassUtils.getMethodForClass(keyFactory, "createKey", 
                        new Class[]{cls,String.class,String.class}).invoke(null, new Object[]{parent,kind,appId});
                }

                return ClassUtils.getMethodForClass(keyFactory, "createKey",
                    new Class[]{String.class,String.class}).invoke(null, new Object[]{kind,appId});
            }

            long id = jsonobj.getLong("id");
            String kind = jsonobj.getString("kind");
            Class keyFactory = clr.classForName("com.google.appengine.api.datastore.KeyFactory",
                cls.getClassLoader(), false);
            if (parent != null)
            {
                return ClassUtils.getMethodForClass(keyFactory, "createKey",
                    new Class[]{cls, String.class, long.class}).invoke(null, new Object[]{parent,kind,Long.valueOf(id)});
            }

            return ClassUtils.getMethodForClass(keyFactory, "createKey", 
                new Class[]{String.class, long.class}).invoke(null, new Object[]{kind,Long.valueOf(id)});
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}