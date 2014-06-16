package org.datanucleus.store.json.orgjson;

/**
 * The JSONException is thrown by the JSON.org classes then things are amiss.
 * @author JSON.org
 * @version 2
 */
public class JSONException extends Exception
{
    private static final long serialVersionUID = -1475438336449433698L;
    private Throwable cause;

    /**
     * Constructs a JSONException with an explanatory message.
     * @param message Detail about the reason for the exception.
     */
    public JSONException(String message)
    {
        super(message);
    }

    public JSONException(Throwable t)
    {
        super(t.getMessage());
        this.cause = t;
    }

    public synchronized Throwable getCause()
    {
        return this.cause;
    }
}
