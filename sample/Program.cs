using System.IO.Compression;
using DoIt.S3Client;

try
{
    using var client = new S3Client(new Uri("http://s3:9090/test/"), "foo-bar", "access-key", "secret-key");

    using (var writer = new StreamWriter(new BrotliStream(await client.OpenObjectForWritingAsync("test.txt.br", "text/plain", new string[] { "br" }), CompressionMode.Compress)))
    {
        await writer.WriteLineAsync("Hello, this is a test object.");
        await writer.WriteLineAsync("The final size of the object is not known when upload is initiated.");
    }

    var metadata = await client.GetObjectMetadataAsync("test.txt.br");

    using (var reader = new StreamReader(new BrotliStream(await client.OpenObjectForReadingAsync("test.txt.br"), CompressionMode.Decompress)))
    {
        Console.Write(await reader.ReadToEndAsync());
    }
    
    await client.DeleteObjectAsync("test.txt.br");
    metadata = await client.GetObjectMetadataAsync("test.txt.br"); // Will return null.
}
catch (S3Exception e)
{
    Console.WriteLine(e.Message);
}
