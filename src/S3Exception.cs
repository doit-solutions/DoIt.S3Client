using System.Net;

namespace DoIt.S3Client;

public class S3Exception : Exception
{
    public S3Exception(string message, HttpStatusCode statusCode) : base(message)
    {
        StatusCode = statusCode;
    }

    public HttpStatusCode StatusCode { get; private init; }
}
