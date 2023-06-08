# do IT S3 Client
[![NuGet Badge](https://buildstats.info/nuget/DoIt.S3Client)](https://www.nuget.org/packages/DoIt.S3Client/)
A simple S3 (Simple Storage Service) client with support for uploading objects of unknown size.

## Why on earth would I want to upload objects of unknown size?!
Well, you might, for example, want to compress a file/stream on the fly while uploading it to you S3 bucket.

## Good point! How would I do that?
```c#
using DoIt.S3Client;

// Create a client, the main interface to you S3 bucket.
using var client = new S3Client(new Uri("https://your-bucket-endpoint"), "your-region", "access-key", "secret-key");

// Open a stream for uploading data of (beforehand) unknown size, such as a ZIP archive created on the fly.
using (var zip = new ZipArchive(await client.OpenObjectForWritingAsync("test.zip", "application/zip"), ZipArchiveMode.Create))
{
    // Dynamically add data to you ZIP archive until you've written all you need to write.
    var entry = zip.CreateEntry("test.txt");
    using var writer = new StreamWriter(entry.Open());
    await writer.WriteLineAsync("Hello, this is a test object.");
}
```

## How does it work?
The client initiates a multipart upload when an S3 stream is created. The "current" part is held in memory and sent to your S3 bucket only when either the current part reaches the maximum part size or the stream is closed/disposed.

Even if you upload very large files to your S3 bucket, only a single part will be held in memory at any time.

## What other operations does the client support?
The primary rationale for this client is to be able to upload objects of unknown sizes in a simple way. The support for other operations is rather limited. The only supported operations are

 * Upload an object
 * Download an object
 * Get an object's metadata
 * Delete an object

 Specifically, bucket operations (for example, creating a bucket) is not supported by this client.