package replicant;

import command.Command.CommandType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class HTTPRequest {

  private static final String CONTENT_LENGTH_HEADER = "Content-Length";
  private static final String CONTENT_TYPE = "Content-type";
  private final String method;
  private final String uri;
  private final String version;
  private final String body;
  private final Map<String, String> headers = new HashMap<>();
  private final Map<String, String> params = new HashMap<>();

  public HTTPRequest(InputStream inputStream) throws IOException {

    BufferedReader in = new BufferedReader(new InputStreamReader(
        inputStream));
    try {
      String requestLine = in.readLine();
      System.out.println(requestLine);
      if (requestLine == null) {
        System.err.println("request is null");
        throw new IOException();
      }
      String[] requestLineTokens = requestLine.split(" ");
      if (requestLineTokens.length != 3) {
        System.err.println("requestLineTokens.length!=3");
        throw new IOException();
      }
      this.method = requestLineTokens[0];
      String[] uriTokens = requestLineTokens[1].split("\\?");
      this.uri = uriTokens[0];
      if (uriTokens.length > 1) {
        String[] paramsToken = uriTokens[1].split("&");
        for (String sParam : paramsToken) {
          String[] param = sParam.split("=");
          this.params.put(param[0], param[1]);
        }
      }

      this.version = requestLineTokens[2];

      String headerLine = in.readLine();
      String[] headerTokens = null;
      while (!headerLine.equals("")) {
        headerTokens = headerLine.split(": ");
        headers.put(headerTokens[0], headerTokens[1]);
        headerLine = in.readLine();
      }

      int contentLength = 0;

      if (headers.get(HTTPRequest.CONTENT_LENGTH_HEADER) != null) {
        contentLength = Integer.parseInt(headers.get(HTTPRequest.CONTENT_LENGTH_HEADER));
      }

      char[] buff = new char[contentLength];

      in.read(buff, 0, buff.length);

      this.body = new String(buff);

      System.out.println("----------- httpserver.HTTPRequest -----------\n" + this
          + "\n--------------------------------\n");

    } catch (IOException e) {
      e.printStackTrace();
      throw new IOException();
    }
  }

  public String getUri() {
    return uri;
  }

  public String toString() {
    String request = method + " " + uri + " " + version + "\r\n";

    for (Map.Entry<String, String> header : headers.entrySet()) {
      request += header.getKey() + ": " + header.getValue() + "\r\n";
    }
    request += "\r\n";
    request += body;
    return request;
  }

  public String getMethod() {
    return method;
  }

  public CommandType getComandType() {
    return switch (this.method) {
      case "Get" -> CommandType.kGet;
      case "Put" -> CommandType.kPut;
      case "Delete" -> CommandType.kDel;
      default -> null;
    };
  }

  public Map<String, String> getParams() {
    return params;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public String getContent() {
    return body;
  }
}