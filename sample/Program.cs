using System.IO.Compression;
using DoIt.S3Client;

try
{
    using var client = new S3Client(new Uri("http://s3:9090/"), "fr-par", "foo", "bar");
    using (var zip = new ZipArchive(await client.OpenObjectForWritingAsync("test/test.zip", "application/zip"), ZipArchiveMode.Create))
    {
        var entry = zip.CreateEntry("test.txt");
        using var writer = new StreamWriter(entry.Open());
        await writer.WriteLineAsync("Hello, this is a test object.");
        await writer.WriteLineAsync("The final size of the object is not known when upload is initiated.");
    }

    var metadata = await client.GetObjectMetadataAsync("test/test.zip");

    using (var zip = new ZipArchive(await client.OpenObjectForReadingAsync("test/test.zip"), ZipArchiveMode.Read))
    {
        var entry = zip.Entries.First();
        using var reader = new StreamReader(entry.Open());
        Console.Write(await reader.ReadToEndAsync());
    }
    await client.DeleteObjectAsync("test/test.zip");
    metadata = await client.GetObjectMetadataAsync("test/test.zip");
}
catch (S3Exception e)
{
    Console.WriteLine(e.Message);
}
