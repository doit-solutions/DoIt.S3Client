using System.Net;

namespace DoIt.S3Client;

public class S3Exception(string message, HttpStatusCode statusCode) : Exception(message)
{
    public HttpStatusCode StatusCode => statusCode;
}
