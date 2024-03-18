using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;

const string inputFilePath = @"D:\Coding\OpenSource\1brc\data\measurements.txt";
const int memoryChunkSize = 64;

var sw = Stopwatch.StartNew();
var dict = new ConcurrentDictionary<string, TempSet>();

using var fh = File.OpenHandle(inputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.None);
var fileLength = RandomAccess.GetLength(fh);
var chunkLength = fileLength / memoryChunkSize;
using var mmf = MemoryMappedFile.CreateFromFile(fh, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true);

var trails = Enumerable.Range(0, memoryChunkSize)
    .AsParallel()
    .Select(chunkIndex =>
    {
        var offset = chunkIndex * fileLength / memoryChunkSize;
        
        return (chunkIndex, ProcessMmfChunk(offset - (chunkIndex > 0 ? chunkIndex - 1 : 0), chunkLength, mmf));
    })
    .OrderBy(x => x.chunkIndex)
    .ToArray();

for (var i = 1; i < trails.Length; ++i)
{
    var combinedSection = trails[i - 1].Item2.BackTrail?.Trim('\0') + trails[i].Item2.FrontTrail?.Trim('\0');
    ParseLine(combinedSection);
}

sw.Stop();
Console.WriteLine(sw.Elapsed);
return;

unsafe (string? FrontTrail, string? BackTrail) ProcessMmfChunk(long offset, long length, MemoryMappedFile mmfChunk)
{
    using var fileChunk = mmfChunk.CreateViewAccessor(offset, length, MemoryMappedFileAccess.Read);

    byte* ptr = null;
    fileChunk.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

    var chunkSpan = new ReadOnlySpan<byte>(ptr + fileChunk.PointerOffset, (int)length);
    
    string? frontTrail = null;

    Span<char> currentLine = stackalloc char[48];
    var lineIndex = 0;
    for (var i = 0; i < chunkSpan.Length; i++)
    {
        var t = chunkSpan[i];
        var currentChar = (char) t;

        if (currentChar == '\n')
        {
            if (i - lineIndex == 0)
            {
                frontTrail = new string(currentLine).Trim();
            }
            else
            {
                ParseLine(currentLine);
            }
            lineIndex = 0;
            currentLine.Clear();
            continue;
        }

        currentLine[lineIndex] = currentChar;
        lineIndex++;
    }
    
    var backTrail = new string(currentLine).Trim();
    backTrail = backTrail == "" ? null : backTrail;

    fileChunk.SafeMemoryMappedViewHandle.ReleasePointer();

    return (frontTrail, backTrail);
}

unsafe void ParseLine(ReadOnlySpan<char> lineSpan)
{
    Span<Range> res = stackalloc Range[2];
    var splitCount = lineSpan.Split(res, ';');

    if (splitCount != 2)
    {
        return;
    }
    
    var citySpan = lineSpan[res[0]].ToString();

    var temp = double.Parse(lineSpan[res[1]], CultureInfo.InvariantCulture.NumberFormat);

    if (!dict.TryGetValue(citySpan, out var minmax))
    {
        minmax = new TempSet()
        {
            Max = temp,
            Min = temp,
            Count = 1
        };
        dict[citySpan] = minmax;
        return;
    }

    minmax.Count++;
    minmax.Sum += (decimal)temp;

    if (temp < minmax.Min)
    {
        minmax.Min = temp;
    }
    else if (temp > minmax.Max)
    {
        minmax.Max = temp;
    }
}

class TempSet
{
    public double Min { get; set; }
    
    public double Max { get; set; }
    
    public int Count { get; set; }
    
    public decimal Sum { get; set; }

    public decimal Mean => Sum / Count;
}