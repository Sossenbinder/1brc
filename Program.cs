using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;

var sw = Stopwatch.StartNew();
const string inputFilePath = @"D:\Coding\OpenSource\1brc\data\measurements.txt";

var dict = new ConcurrentDictionary<string, TempSet>();

var index = 0;
var step = 0;

const int chunkSize = 10_000_000;

var chunk = new string[chunkSize];
foreach (var line in File.ReadLines(inputFilePath))
{
    chunk[index % chunkSize] = line;
    index++;

    if (index % chunkSize == 0)
    {
        chunk
            .AsParallel()
            .ForAll(x => RunLoop(x.AsSpan()));
    }

    if (index % 10_000_000 == 0)
    {
        step++;
        Console.WriteLine($"{step}% done");
    }
}
sw.Stop();
Console.WriteLine(sw.Elapsed);
return;

unsafe void RunLoop(ReadOnlySpan<char> lineSpan)
{
    Span<Range> res = stackalloc Range[2];
    lineSpan.Split(res, ';');
    var city = lineSpan[res[0]].ToString();

    var temp = decimal.Parse(lineSpan[res[1]], CultureInfo.InvariantCulture.NumberFormat);

    if (!dict.TryGetValue(city, out var minmax))
    {
        minmax = new TempSet()
        {
            Max = temp,
            Min = temp,
            Count = 1
        };
        dict[city] = minmax;
        return;
    }

    minmax.Count++;
    minmax.Sum += temp;

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
    public decimal Min { get; set; }
    
    public decimal Max { get; set; }
    
    public int Count { get; set; }
    
    public decimal Sum { get; set; }

    public decimal Mean => Sum / Count;
}