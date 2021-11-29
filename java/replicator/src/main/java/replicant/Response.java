package replicant;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class Response {
    public static final String HTML_CONTENT_TYPE = "text/html";
    public static final Response BAD_REQUEST = createBadRequestResponse();
    private static final String DEFAULT_RESPONSE_LINE = "HTTP/1.1 200 OK\r\n";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String CONTENT_LENGTH_HEADER = "Content-Length";
    private static final String DATE_HEADER = "Date";
    private final Map<Integer, String> responseLines = new HashMap<>();
    private final SimpleDateFormat rfc1123 = new SimpleDateFormat("EEE, dd MM yyyy HH:mm:ss z", Locale.US);
    private String responseLine;
    private String body;
    private Map<String, String> headers = new HashMap<>();

    public Response() {
        this.responseLines.put(200, "HTTP/1.1 200 OK\r\n");
        this.responseLines.put(400, "HTTP/1.1 400 Bad httpserver.Request\r\n");
        this.responseLines.put(403, "HTTP/1.1 403 Forbidden\r\n");
        this.responseLines.put(404, "HTTP/1.1 404 Not Found\r\n");
        this.responseLines.put(500, "HTTP/1.1 500 Internal Server Error\r\n");

    }

    public Response(String body) {
        this();
        this.responseLine = getDefaultResponseLine();
        this.headers = getDefaultHeaders();
        this.body = body;
    }

    public Response(int code, String contentType, String content) {
        this();
        this.responseLine = this.responseLines.get(code);
        this.headers.put(CONTENT_TYPE_HEADER, contentType);
        this.headers.put(CONTENT_LENGTH_HEADER, String.valueOf(content.getBytes().length));
        this.headers.put(DATE_HEADER, rfc1123.format(new Date()));
        this.body = content;
    }

    private static Response createBadRequestResponse() {
        return new Response(400, "text/html", "<h1> Bad httpserver.Request </h1>");
    }

    private Map<String, String> getDefaultHeaders() {
        return new HashMap<String, String>();
    }

    private String getDefaultResponseLine() {
        return DEFAULT_RESPONSE_LINE;
    }

    public void setBody(String body) {
        headers.put(CONTENT_LENGTH_HEADER, Integer.toString(body.getBytes().length));
        this.body = body;
    }

    @Override
    public String toString() {
        String response = responseLine;

        for (Map.Entry<String, String> header : headers.entrySet()) {
            response += header.getKey() + ": " + header.getValue() + "\r\n";
        }
        response += "\r\n";
        response += body;
        return response;
    }

    public void setContent(String contentType, String content, String encoding) {
        this.headers.put(CONTENT_TYPE_HEADER, contentType + "; charset=" + encoding);
        setBody(content);

    }

    public void setHeader(String key, String value) {
        this.headers.put(key, value);
    }
}
