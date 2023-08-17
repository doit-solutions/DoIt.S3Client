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

public record MetaData(StorageClass StorageClass, string ETag, long SizeInBytes, string? ContentType, string? ContentEncoding);

public class S3Client : IDisposable
{
    private const string S3ServiceName = "s3";
    private const string S3XmlNamespace = "http://s3.amazonaws.com/doc/2006-03-01/";

    private readonly HttpClient _client;
    private readonly AWS4RequestSigner _signer;

    private readonly Uri _endpoint;
    private readonly string _region;

    public S3Client(Uri endpoint, string region, string accessKey, string secretKey)
    {
        _client = new HttpClient();
        _signer = new AWS4RequestSigner(accessKey, secretKey);

        _endpoint = endpoint;
        _region = region;
    }
    
    public async Task<Stream> OpenObjectForWritingAsync(string key, string contentType, IEnumerable<string>? contentEncoding = null, StorageClass storageClass = StorageClass.Standard, CancellationToken cancellationToken = default)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, new Uri(_endpoint, $"{key}?uploads"));
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
        using var signedReq = await _signer.Sign(req, S3ServiceName, _region);
        using var resp = await _client.SendAsync(signedReq, cancellationToken);
        if (resp.IsSuccessStatusCode)
        {
            using var reader = XmlReader.Create(await resp.Content.ReadAsStreamAsync(cancellationToken), new XmlReaderSettings { Async = true, CloseInput = true, DtdProcessing = DtdProcessing.Ignore, XmlResolver = null });
            while (await reader.ReadAsync())
            {
                if (reader.NodeType == XmlNodeType.Element && string.Equals(reader.LocalName, "UploadId", StringComparison.InvariantCulture)/* && string.Equals(reader.NamespaceURI, S3XmlNamespace, StringComparison.InvariantCulture)*/ && reader.Depth == 1)
                {
                    var uploadId = await reader.ReadElementContentAsStringAsync();
                    return new S3MultipartUploadStream(_client, _endpoint, _region, _signer, key, uploadId);
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
        var req = await _signer.Sign(new HttpRequestMessage(HttpMethod.Get, new Uri(_endpoint, key)), S3ServiceName, _region);
        var resp = await _client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
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
        using var req = await _signer.Sign(new HttpRequestMessage(HttpMethod.Head, new Uri(_endpoint, key)), S3ServiceName, _region);
        using var resp = await _client.SendAsync(req, cancellationToken);
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
        using var req = await _signer.Sign(new HttpRequestMessage(HttpMethod.Delete, new Uri(_endpoint, key)), S3ServiceName, _region);
        using var resp = await _client.SendAsync(req, cancellationToken);
        return resp.StatusCode == System.Net.HttpStatusCode.NoContent;
    }

    public void Dispose()
    {
        _signer.Dispose();
        _client.Dispose();
    }

    private class S3MultipartUploadStream : Stream
    {
        private const int PartSize = 1024 * 1024 * 5;
        
        private readonly MD5 _md5;
        private readonly HttpClient _client;
        private readonly Uri _endpoint;
        private readonly AWS4RequestSigner _signer;
        private readonly string _region;
        private readonly string _key;
        private readonly string _uploadId;
        private readonly IDictionary<int, string> _partETags;
        private readonly byte[] _currentPart;
        private int _currentPartLength;
        private int _currentPartNumber;
        private bool _disposed;

        public S3MultipartUploadStream(HttpClient client, Uri endpoint, string region, AWS4RequestSigner signer, string key, string uploadId)
        {
            _md5 = MD5.Create();
            _client = client;
            _endpoint = endpoint;
            _region = region;
            _signer = signer;
            _key = key;
            _uploadId = uploadId;
            _partETags = new Dictionary<int, string>();
            _currentPart = new byte[PartSize];
            _currentPartLength = 0;
            _currentPartNumber = 1;
            _disposed = false;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Position { get => ((_currentPartNumber - 1) * PartSize) + _currentPartLength; set => throw new NotSupportedException(); }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var bytesCopied = 0;
            while (bytesCopied < count)
            {
                if (_currentPartLength < PartSize)
                {
                    var bytesToCopy = Math.Min(PartSize - _currentPartLength, count - bytesCopied);
                    buffer.AsMemory(offset, bytesToCopy).CopyTo(_currentPart.AsMemory(_currentPartLength, bytesToCopy));
                    bytesCopied += bytesToCopy;
                    _currentPartLength += bytesToCopy;
                }
                if (_currentPartLength == PartSize)
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
            if (!_disposed)
            {
                UploadCurrentPart();
                using (var req = new HttpRequestMessage(HttpMethod.Post, new Uri(_endpoint, $"{_key}?uploadId={HttpUtility.UrlEncode(_uploadId)}")))
                {
                    using var ms = new MemoryStream();
                    using (var writer = XmlWriter.Create(ms, new XmlWriterSettings { Indent = false, Encoding = Encoding.UTF8 }))
                    {
                        writer.WriteStartDocument();
                        writer.WriteStartElement("CompleteMultipartUpload", S3XmlNamespace);
                        foreach (var (partNumber, eTag) in _partETags)
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
                    using var signedReq = _signer.Sign(req, S3ServiceName, _region).ConfigureAwait(false).GetAwaiter().GetResult();
                    using var resp = _client.Send(req);
                    if (!resp.IsSuccessStatusCode)
                    {
                        using var reader = new StreamReader(resp.Content.ReadAsStream());
                        throw new S3Exception(reader.ReadToEnd(), resp.StatusCode);
                    }
                }
                _md5.Dispose();

                _disposed = true;
            }

            base.Dispose(disposing);
        }

        private void UploadCurrentPart()
        {
            if (_currentPartLength > 0)
            {
                using var req = new HttpRequestMessage(HttpMethod.Put, new Uri(_endpoint, $"{_key}?partNumber={_currentPartNumber}&uploadId={HttpUtility.UrlEncode(_uploadId)}"));
                req.Content = new ByteArrayContent(_currentPart, 0, _currentPartLength);
                req.Content.Headers.ContentMD5 = _md5.ComputeHash(_currentPart, 0, _currentPartLength);
                using var signedReq = _signer.Sign(req, S3ServiceName, _region).ConfigureAwait(false).GetAwaiter().GetResult();
                using var resp = _client.Send(req);
                var tmp = new StreamReader(resp.Content.ReadAsStream()).ReadToEnd();
                _partETags.Add(_currentPartNumber, resp.EnsureSuccessStatusCode().Headers.ETag?.Tag ?? throw new S3Exception("Multipart upload response contains no ETag header.", resp.StatusCode));
                _currentPartNumber += 1;
                _currentPartLength = 0;
            }
        }
    }

    private class HttpResponseStream : Stream
    {
        private readonly HttpRequestMessage _request;
        private readonly HttpResponseMessage _response;
        private readonly Stream _stream;

        public HttpResponseStream(HttpRequestMessage request, HttpResponseMessage response, Stream stream)
        {
            _request = request;
            _response = response;
            _stream = stream;
        }

        public override bool CanRead => _stream.CanRead;
        public override bool CanSeek => _stream.CanSeek;
        public override bool CanWrite => _stream.CanWrite;
        public override long Length => _stream.Length;
        public override long Position { get => _stream.Position; set => _stream.Position = value; }
        public override void Flush() => _stream.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _stream.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => _stream.Seek(offset, origin);
        public override void SetLength(long value) => _stream.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _stream.Write(buffer, offset, count);

        protected override void Dispose(bool disposing)
        {
            _stream.Dispose();
            _response.Dispose();
            _request.Dispose();

            base.Dispose(disposing);
        }
    }
}
