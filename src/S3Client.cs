using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using System.Xml;
using Aws4RequestSigner;

namespace DoIt.S3Client;

public enum StorageClass
{
    Standard,
    Standard_InfrequentAccess,
    OneZone_InfrequentAccess,
    IntelligentTiering,
    Archive_FlexibleRetrieval,
    Archive_InstantRetrieval,
    Archive_DeepArchive,
    [Obsolete("It is recommend not using this storage class. The S3 standard storage class is more cost-effective. See https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html.")]
    ReducedRedundency
}

public enum ObjectVisibility
{
    Private,
    PublicRead,
    PublicReadWrite

}

public record MetaData(StorageClass StorageClass, string ETag, long SizeInBytes, string? ContentType, string? ContentEncoding);

public class S3Client(Uri endpoint, string region, string accessKey, string secretKey) : IDisposable
{
    const string S3ServiceName = "s3";
    const string S3XmlNamespace = "http://s3.amazonaws.com/doc/2006-03-01/";

    readonly HttpClient client = new();
    readonly AWS4RequestSigner signer = new(accessKey, secretKey);

    public async Task<Stream> OpenObjectForWritingAsync(string key, string contentType, IEnumerable<string>? contentEncoding = null, StorageClass storageClass = StorageClass.Standard, ObjectVisibility visibility = ObjectVisibility.Private, CancellationToken cancellationToken = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, new Uri(endpoint, $"{key}?uploads"));
        req.Content = new StringContent(string.Empty, new System.Net.Http.Headers.MediaTypeHeaderValue(contentType));
        if (contentEncoding?.Any() ?? false)
        {
            foreach (var enc in contentEncoding)
            {
                req.Content.Headers.ContentEncoding.Add(enc);
            }
        }
        req.Content.Headers.Add("x-amz-storage-class", storageClass switch
        {
            StorageClass.Standard => "STANDARD",
            StorageClass.Standard_InfrequentAccess => "STANDARD_IA",
            StorageClass.OneZone_InfrequentAccess => "ONEZONE_IA",
            StorageClass.IntelligentTiering => "INTELLIGENT_TIERING",
            StorageClass.Archive_FlexibleRetrieval => "GLACIER",
            StorageClass.Archive_InstantRetrieval => "GLACIER_IR",
            StorageClass.Archive_DeepArchive => "DEEP_ARCHIVE",
#pragma warning disable 0618
            StorageClass.ReducedRedundency => "REDUCED_REDUNDANCY",
#pragma warning restore 0618
            _ => "STANDARD"
        });
        req.Content.Headers.Add("x-amz-acl", visibility switch
        {
            ObjectVisibility.PublicRead => "public-read",
            ObjectVisibility.PublicReadWrite => "public-read-write",
            _ => "private"
        });
        using var signedReq = await signer.Sign(req, S3ServiceName, region);
        using var resp = await client.SendAsync(signedReq, cancellationToken);
        if (resp.IsSuccessStatusCode)
        {
            using var reader = XmlReader.Create(await resp.Content.ReadAsStreamAsync(cancellationToken), new XmlReaderSettings { Async = true, CloseInput = true, DtdProcessing = DtdProcessing.Ignore, XmlResolver = null });
            while (await reader.ReadAsync())
            {
                if (reader.NodeType == XmlNodeType.Element && string.Equals(reader.LocalName, "UploadId", StringComparison.InvariantCulture)/* && string.Equals(reader.NamespaceURI, S3XmlNamespace, StringComparison.InvariantCulture)*/ && reader.Depth == 1)
                {
                    var uploadId = await reader.ReadElementContentAsStringAsync();
                    return new S3MultipartUploadStream(client, endpoint, region, signer, key, uploadId);
                }
            }
        }
        else
        {
            using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(cancellationToken));
            throw new S3Exception(await reader.ReadToEndAsync(cancellationToken), resp.StatusCode);
        }

        throw new S3Exception("Failed to find UploadId in response XML.", resp.StatusCode);
    }

    public async Task<Stream> OpenObjectForReadingAsync(string key, CancellationToken cancellationToken = default)
    {
        var req = await signer.Sign(new HttpRequestMessage(HttpMethod.Get, new Uri(endpoint, key)), S3ServiceName, region);
        var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (resp.IsSuccessStatusCode)
        {
            return new HttpResponseStream(req, resp, await resp.Content.ReadAsStreamAsync(cancellationToken));
        }
        else
        {
            using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(cancellationToken));
            throw new S3Exception(await reader.ReadToEndAsync(cancellationToken), resp.StatusCode);
        }
    }

    public async Task<MetaData?> GetObjectMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        using var req = await signer.Sign(new HttpRequestMessage(HttpMethod.Head, new Uri(endpoint, key)), S3ServiceName, region);
        using var resp = await client.SendAsync(req, cancellationToken);
        if (resp.IsSuccessStatusCode)
        {
            return new MetaData
            (
                resp.Headers.Contains("x-amz-storage-class") ? resp.Headers.GetValues("x-amz-storage-class").FirstOrDefault() switch
                {
                    "STANDARD" => StorageClass.Standard,
                    "STANDARD_IA" => StorageClass.Standard_InfrequentAccess,
                    "ONEZONE_IA" => StorageClass.OneZone_InfrequentAccess,
                    "INTELLIGENT_TIERING" => StorageClass.IntelligentTiering,
                    "GLACIER" => StorageClass.Archive_FlexibleRetrieval,
                    "GLACIER_IR" => StorageClass.Archive_InstantRetrieval,
                    "DEEP_ARCHIVE" => StorageClass.Archive_DeepArchive,
#pragma warning disable 0618
                    "REDUCED_REDUNDANCY" => StorageClass.ReducedRedundency,
#pragma warning restore 0618
                    _ => StorageClass.Standard
                } : StorageClass.Standard,
                resp.Headers.ETag?.Tag ?? string.Empty,
                resp.Content.Headers.ContentLength ?? 0,
                resp.Content.Headers.ContentType?.ToString(),
                resp.Content.Headers.ContentEncoding?.ToString()
            );
        }
        else if (resp.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
        else
        {
            using var reader = new StreamReader(await resp.Content.ReadAsStreamAsync(cancellationToken));
            throw new S3Exception(await reader.ReadToEndAsync(cancellationToken), resp.StatusCode);
        }
    }

    public async Task<bool> DeleteObjectAsync(string key, CancellationToken cancellationToken = default)
    {
        using var req = await signer.Sign(new HttpRequestMessage(HttpMethod.Delete, new Uri(endpoint, key)), S3ServiceName, region);
        using var resp = await client.SendAsync(req, cancellationToken);
        return resp.StatusCode == System.Net.HttpStatusCode.NoContent;
    }

    public void Dispose()
    {
        signer.Dispose();
        client.Dispose();
    }

    class S3MultipartUploadStream(HttpClient client, Uri endpoint, string region, AWS4RequestSigner signer, string key, string uploadId) : Stream
    {
        const int PartSize = 1024 * 1024 * 5;

        readonly MD5 md5 = MD5.Create();
        readonly IDictionary<int, string> partETags = new Dictionary<int, string>();
        readonly byte[] currentPart = new byte[PartSize];
        int currentPartLength = 0;
        int currentPartNumber = 1;
        bool disposed = false;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Position { get => ((currentPartNumber - 1) * PartSize) + currentPartLength; set => throw new NotSupportedException(); }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var bytesCopied = 0;
            while (bytesCopied < count)
            {
                if (currentPartLength < PartSize)
                {
                    var bytesToCopy = Math.Min(PartSize - currentPartLength, count - bytesCopied);
                    buffer.AsMemory(offset, bytesToCopy).CopyTo(currentPart.AsMemory(currentPartLength, bytesToCopy));
                    bytesCopied += bytesToCopy;
                    currentPartLength += bytesToCopy;
                }
                if (currentPartLength == PartSize)
                {
                    UploadCurrentPart();
                }
            }
        }

        public override void Flush()
        {
        }

        public override long Length => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (!disposed)
            {
                UploadCurrentPart();
                using (var req = new HttpRequestMessage(HttpMethod.Post, new Uri(endpoint, $"{key}?uploadId={HttpUtility.UrlEncode(uploadId)}")))
                {
                    using var ms = new MemoryStream();
                    using (var writer = XmlWriter.Create(ms, new XmlWriterSettings { Indent = false, Encoding = Encoding.UTF8 }))
                    {
                        writer.WriteStartDocument();
                        writer.WriteStartElement("CompleteMultipartUpload", S3XmlNamespace);
                        foreach (var (partNumber, eTag) in partETags)
                        {
                            writer.WriteStartElement("Part", S3XmlNamespace);
                            writer.WriteElementString("PartNumber", S3XmlNamespace, partNumber.ToString(CultureInfo.InvariantCulture));
                            writer.WriteElementString("ETag", S3XmlNamespace, eTag);
                            writer.WriteEndElement(); // Part
                        }
                        writer.WriteEndElement(); // CompleteMultipartUpload
                        writer.WriteEndDocument();
                    }
                    req.Content = new ByteArrayContent(ms.ToArray());
                    req.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/xml", "utf-8");
                    using var signedReq = signer.Sign(req, S3ServiceName, region).ConfigureAwait(false).GetAwaiter().GetResult();
                    using var resp = client.Send(req);
                    if (!resp.IsSuccessStatusCode)
                    {
                        using var reader = new StreamReader(resp.Content.ReadAsStream());
                        throw new S3Exception(reader.ReadToEnd(), resp.StatusCode);
                    }
                }
                md5.Dispose();

                disposed = true;
            }

            base.Dispose(disposing);
        }

        void UploadCurrentPart()
        {
            if (currentPartLength > 0)
            {
                using var req = new HttpRequestMessage(HttpMethod.Put, new Uri(endpoint, $"{key}?partNumber={currentPartNumber}&uploadId={HttpUtility.UrlEncode(uploadId)}"));
                req.Content = new ByteArrayContent(currentPart, 0, currentPartLength);
                req.Content.Headers.ContentMD5 = md5.ComputeHash(currentPart, 0, currentPartLength);
                using var signedReq = signer.Sign(req, S3ServiceName, region).ConfigureAwait(false).GetAwaiter().GetResult();
                using var resp = client.Send(req);
                var tmp = new StreamReader(resp.Content.ReadAsStream()).ReadToEnd();
                partETags.Add(currentPartNumber, resp.EnsureSuccessStatusCode().Headers.ETag?.Tag ?? throw new S3Exception("Multipart upload response contains no ETag header.", resp.StatusCode));
                currentPartNumber += 1;
                currentPartLength = 0;
            }
        }
    }

    class HttpResponseStream(HttpRequestMessage request, HttpResponseMessage response, Stream stream) : Stream
    {
        public override bool CanRead => stream.CanRead;
        public override bool CanSeek => stream.CanSeek;
        public override bool CanWrite => stream.CanWrite;
        public override long Length => stream.Length;
        public override long Position { get => stream.Position; set => stream.Position = value; }
        public override void Flush() => stream.Flush();
        public override int Read(byte[] buffer, int offset, int count) => stream.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => stream.Seek(offset, origin);
        public override void SetLength(long value) => stream.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => stream.Write(buffer, offset, count);

        protected override void Dispose(bool disposing)
        {
            stream.Dispose();
            response.Dispose();
            request.Dispose();

            base.Dispose(disposing);
        }
    }
}
