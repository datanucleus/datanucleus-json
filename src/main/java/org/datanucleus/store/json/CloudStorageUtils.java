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

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;

public class CloudStorageUtils
{
    private static final String UTF8_CHARSET = "UTF-8";
    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

    public static boolean isMemberNested(AbstractMemberMetaData mmd)
    {
        boolean nested = false;
        String nestedStr = mmd.getValueForExtension("nested");
        if (nestedStr != null && nestedStr.equalsIgnoreCase("" + (!nested)))
        {
            nested = !nested;
        }

        return nested;
    }

    /**
     * Compile the HMAC of the data using the secret key
     * @param key the secret key
     * @param data data
     * @return The HMAC
     */
    public static String hmac(String key, String data)
    {
        try
        {
            byte[] secretyKeyBytes = key.getBytes(UTF8_CHARSET);
            SecretKeySpec secretKeySpec = new SecretKeySpec(secretyKeyBytes, HMAC_SHA1_ALGORITHM);
            Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
            mac.init(secretKeySpec);
            byte[] rawHmac = mac.doFinal(data.getBytes(UTF8_CHARSET));
            return new String(org.datanucleus.util.Base64.encode(rawHmac));
        }
        catch (UnsupportedEncodingException e)
        {
            throw new NucleusException(UTF8_CHARSET + " is unsupported!", e);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        catch (InvalidKeyException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    /**
     * get the date according to the HTTP standard
     * @return the current date
     */
    public static String getHTTPDate()
    {
        DateFormat httpDateFormat = new SimpleDateFormat("EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' 'Z", Locale.ENGLISH);
        httpDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return httpDateFormat.format(new Date());
    }
}