# Analyzing German Weather Data using Open Data, .NET and SQL #

This is the repository for the Article *Analyzing German Weather Data using Open Data, .NET and SQL*:

* https://www.bytefish.de/blog/weather_data_timeseries_analysis.html

[Link DWD]: https://social.bund.de/@DeutscherWetterdienst/111051263708804194
[Link SZ]: https://www.sueddeutsche.de/panorama/wetter-essen-sieben-hitzetage-am-stueck-september-rekord-an-nrw-stationen-dpa.urn-newsml-dpa-com-20090101-230911-99-154458

The Deutsche Wetterdienst (DWD) has recently shared an interesting quote, which reads:

> Kein Grund zum Jubeln in Zeiten der #Klimakrise: "Sieben Hitzetage am Stück: 
> September-Rekord an NRW-Stationen" bestätigt der DWD der Süddeutschen 
> Zeitung. ([Link DWD])

This literally means September was a record month in Germany, with 7 consecutive days 
of severe heat. In the referenced article at Süddeutsche Zeitung (SZ) we can find the 
criteria applied:

> Die vergangenen heißen Septembertage haben an einigen Orten in Nordrhein-Westfalen laut 
> dem Deutschen Wetterdienst für einen Hitzerekord in dem Monat gesorgt. An den Stationen 
> Bochum, Weilerswist-Lommersum und Tönisvorst sei mit dem Montag der siebte heiße Tag mit 
> mehr als 30 Grad hintereinander erreicht, sagte Thomas Kesseler-Lauterkorn, Meteorologe 
> beim Deutschen Wetterdienst (DWD). ([Link SZ])

So we are looking for a streak of at least 7 days, with an air temperature of at least 
30 degrees celsius for the stations *Bochum*, *Weilerswist-Lommersum* and *Tönisvorst*.

The Deutsche Wetterdienst (DWD) shares the measured temperatures with a 10 minute accuracy, 
for both recent (`/recent`) and historical (`/historical`) measurements at:

* [https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/)

So let's see how we could identify those streaks in the data!

## Table of contents ##

[TOC]

## What we are going to build ##

We are going to build an application for downloading and efficiently inserting the DWD weather data 
into an SQL Server database. This database is used to run queries and trying to find the September 
record streaks.

All code can be found in the Git Repository at:

* [https://github.com/bytefish/DwdAnalysis](https://github.com/bytefish/DwdAnalysis)


## Using SQL to find the Consecutive Heat Days ##

In the following section we can see how to import the data using the DWD Open Data and .NET, but 
to not get lost in technical details, let's assume the hardest part of writing the data to the 
RDBMS is done.

### Understanding the Database Schema ##

The database is going to have two tables 

* `[dbo].[Station]` for the Stations
* `[dbo].[Messwert]` for the Measurements

In T-SQL the `[dbo].[Station]` table is created like this:

```sql
CREATE TABLE [dbo].[Station](
    [StationID]         [nchar](5) NOT NULL,
    [DatumVon]          [datetime2](7) NULL,
    [DatumBis]          [datetime2](7) NULL,
    [Stationshoehe]     [real] NULL,
    [GeoBreite]         [real] NULL,
    [GeoLaenge]         [real] NULL,
    [Stationsname]      [nvarchar](255) NULL,
    [Bundesland]        [nvarchar](255) NULL,
    CONSTRAINT [PK_Station] PRIMARY KEY CLUSTERED 
    (
        [StationID] ASC
    )
) ON [PRIMARY]
```

In T-SQL the `[dbo].[Messwert]` table is created like this:

```sql
CREATE TABLE [dbo].[Messwert](
    [StationID]     [nchar](5) NOT NULL,
    [MessDatum]     [datetime2](7) NOT NULL,
    [QN]            [int] NULL,
    [PP_10]         [real] NULL,
    [TT_10]         [real] NULL,
    [TM5_10]        [real] NULL,
    [RF_10]         [real] NULL,
    [TD_10]         [real] NULL
) ON [PRIMARY]
```

We can now have fun with the data, Common Table Expressions (CTE) and WINDOW functions allow 
to write a very clean query to identify heat streaks.

I came up with the following SQL:

```sql
-- Start by finding the maximum temperature per day, because the [MessDatum] is 
-- given in 10 Minute accuracy. We are using TT_10 Measurement, which is the Air 
-- Temperature 2m above the ground.
WITH MaxTempByStationAndDay AS (
    SELECT 
        [StationID], CAST([MessDatum] AS DATE) AS [MessDatum], MAX([TT_10]) AS [Temperature]
    FROM 
        [dbo].[Messwert]
    GROUP BY 
        [StationID], CAST([MessDatum] AS DATE)
),
-- Only Select the Days with more than 30 Degrees, according to 
-- the metrics applied by the Süddeutsche Zeitung article.
MaxTempByStationAndDayAbove30 AS (
    SELECT 
        [StationID], [MessDatum], [Temperature]
    FROM 
        [MaxTempByStationAndDay]
    WHERE 
        [Temperature] >= 30
),
-- We want to find the consecutive days, so we are putting all measurements in a 
-- DateGroup they belong to. 
TemperatureGroups AS (
    SELECT 
        RANK() OVER (PARTITION BY [StationID] ORDER BY [MessDatum]) AS RowNumber
        , [StationID]
        , [MessDatum]
        , DATEADD(day, -RANK() OVER (PARTITION BY [StationID] ORDER BY [MessDatum]), MessDatum) AS DateGroup
        , [Temperature]
        FROM [MaxTempByStationAndDayAbove30]
),
-- We can now calculate the number of consecutive days, so 
-- we can verify the results reported by DWD and SZ. 
HeatStreaks AS (
    SELECT 
        [StationID]
        , COUNT(*) AS [ConsecutiveDays]
        , MIN([MessDatum]) AS [StartOfHeatStreak]
        , MAX([MessDatum]) AS [EndOfHeatStreak]
    FROM 
        [TemperatureGroups]
    GROUP BY 
        [StationID], [DateGroup]
)
SELECT 
    station.Stationsname, station.Bundesland, heat_streak.* 
FROM 
    [HeatStreaks] heat_streak
        INNER JOIN [dbo].[Station] station ON heat_streak.StationID = station.StationID
WHERE 
    ConsecutiveDays >= 7
ORDER BY 
    Stationsname
```

And we get the following results:

```
Stationsname                    StationID    ConsecutiveDays    StartOfHeatStreak    EndOfHeatStreak

Barsinghausen-Hohenbostel       00294            7                2023-09-05            2023-09-11
Bochum                          00555            7                2023-09-05            2023-09-11
Huy-Pabstorf                    06266            7                2023-09-05            2023-09-11
Mannheim                        05906            7                2023-09-06            2023-09-12
Mergentheim, Bad-Neunkirchen    03257            8                2016-09-08            2016-09-15
Tönisvorst                      05064            7                2023-09-05            2023-09-11
Waghäusel-Kirrlach              05275            9                2023-09-04            2023-09-12
Weilerswist-Lommersum           01327            7                2023-09-05            2023-09-11
```

### Conclusion ###

We have been able to identify the consecutive days of extreme heat in September for the Stations 
*Bochum*, *Tönisvorst* and *Weilerswist-Lommersum*. We can also see in the data, that 2023 is 
indeed a record year, with only 2016 having a streak of at least 7 days.

If you have ideas for more queries, let me know.


## Open Data, SQL Server and .NET ## 

In this section we will see how to import the DWD Open Data using an SQL Server and .NET.

### On Efficiently Importing Data to SQL Server ###

[Microsoft Learn: BULK INSERT (Transact-SQL)]: https://learn.microsoft.com/de-de/sql/t-sql/statements/bulk-insert-transact-sql?view=sql-server-ver16
[SqlBulkCopy]: https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/bulk-copy-operations-in-sql-server

A golden rule when working with any kind of data is to *never trust the incoming data*. There's 
missing data, malformed data, duplicate measurements, ... to name a few issues. Let's summarize 
my personal experience.

The T-SQL `BULK INSERT` statement can directly import a CSV file, see [Microsoft Learn: BULK INSERT (Transact-SQL)] 
for more information. It sounds very, very appealing first, but what if incoming text data contains formats the SQL 
Server doesn't understand? It falls flat.

Spoiler alarm: The DWD data is no exception.

What's most flexible, very efficient and easy to implement is to use a Stored Procedure, that takes a Table-Valued 
Parameter as its input variable. In the application we can pre-process the formats and in the Stored Procedure we 
can use all SQL statements required.

## Importing the DWD Measurements ##

### Understanding the Database Schema ##

We start by defining the `[dbo].[Station]` table, which is going to hold 
all information about the weather station, such as the station name, measurement 
period or its exact location.

```sql
CREATE TABLE [dbo].[Station](
    [StationID]         [nchar](5) NOT NULL,
    [DatumVon]          [datetime2](7) NULL,
    [DatumBis]          [datetime2](7) NULL,
    [Stationshoehe]     [real] NULL,
    [GeoBreite]         [real] NULL,
    [GeoLaenge]         [real] NULL,
    [Stationsname]      [nvarchar](255) NULL,
    [Bundesland]        [nvarchar](255) NULL,
    CONSTRAINT [PK_Station] PRIMARY KEY CLUSTERED 
    (
        [StationID] ASC
    )
) ON [PRIMARY]
```

The weather measurements are going into the table `[dbo].[Messwert]` and holds 
all measurements given in the zipped CSV files:

```sql
CREATE TABLE [dbo].[Messwert](
    [StationID]     [nchar](5) NOT NULL,
    [MessDatum]     [datetime2](7) NOT NULL,
    [QN]            [int] NULL,
    [PP_10]         [real] NULL,
    [TT_10]         [real] NULL,
    [TM5_10]        [real] NULL,
    [RF_10]         [real] NULL,
    [TD_10]         [real] NULL
) ON [PRIMARY]
```

We then define the Table Types, that will be the input parameter for 
the Stored Procedures to come. The `[dbo].[udt_StationType]` is 
basically a one to one mapping to the `[dbo].[Station]` table.

```sql
CREATE TYPE [dbo].[udt_StationType] AS TABLE (
    [StationID]         [nchar](5),
    [DatumVon]          [datetime2](7),
    [DatumBis]          [datetime2](7),
    [Stationshoehe]     [real],
    [GeoBreite]         [real],
    [GeoLaenge]         [real],
    [Stationsname]      [nvarchar](255),
    [Bundesland]        [nvarchar](255)
);
```

We'll define the `[dbo].[udt_MesswertType]` accordingly, which represents the 
weather measurements. Again it's a one to one mapping to the `[dbo].[Messwert]` 
table.

```sql
CREATE TYPE [dbo].[udt_MesswertType] AS TABLE (
    [StationID]     [nchar](5),
    [MessDatum]     [datetime2](7),
    [QN]            [int],
    [PP_10]         [real],
    [TT_10]         [real],
    [TM5_10]        [real],
    [RF_10]         [real],
    [TD_10]         [real]
);
```

What's left is to define the Stored Procedures for inserting the measurements. I've initially 
tried to avoid duplicate data, that's in the Stored Procedures. But working with `MERGE` when 
being multi-threaded is... hell. 

So we are down to basic inserts for the Stations:

```sql
CREATE OR ALTER PROCEDURE [dbo].[usp_InsertStation]
   @Entities [dbo].[udt_StationType] ReadOnly
AS
BEGIN
    
    SET NOCOUNT ON;

   INSERT INTO [dbo].[Station](StationID, DatumVon, DatumBis, Stationshoehe, GeoBreite, GeoLaenge, Stationsname, Bundesland)
   SELECT StationID, DatumVon, DatumBis, Stationshoehe, GeoBreite, GeoLaenge, Stationsname, Bundesland
   FROM @Entities;

END
```

And we do the same for the Measurements:

```sql
CREATE OR ALTER PROCEDURE [dbo].[usp_InsertMesswert]
    @Entities [dbo].[udt_MesswertType] ReadOnly
AS
BEGIN
    
    SET NOCOUNT ON;

    INSERT INTO [dbo].[Messwert](StationID, MessDatum, QN, PP_10, TT_10, TM5_10, RF_10, TD_10)
    SELECT StationID, MessDatum, QN, PP_10, TT_10, TM5_10, RF_10, TD_10
    FROM @Entities;

END
```

And that's it for the database.

### Understanding the File Formats ###

Before we begin the implementation, we'll take a look at the files for stations and measurements. 

The stations are given in a file `zehn_min_tu_Beschreibung_Stationen.txt` in the data folder, the first few lines look like this:

```
Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland
----------- --------- --------- ------------- --------- --------- ----------------------------------------- ----------
00003 19930429 20110331            202     50.7827    6.0941 Aachen                                   Nordrhein-Westfalen
00044 20070209 20230918             44     52.9336    8.2370 Großenkneten                             Niedersachsen
...
```

The first two lines are just information and need to be skipped, when reading the data. We can see, that the 
properties are given in a fixed-width format. So how could we extract these fields? Regular Expressions with 
named captures seems like a nice way for me.

After counting the width for each column, I came up with this simple Regular Expression:

```
(?<stations_id>.{5})\\s(?<von_datum>.{8})\\s(?<bis_datum>.{8})\\s(?<stationshoehe>.{14})\\s(?<geo_breite>.{11})\\s(?<geo_laenge>.{9})\\s(?<stationsname>.{40})\\s(?<bundesland>.+)$
```

Cool!

The measurements are given in ZIP files. These ZIP files contain a single `txt` file with the 
measurements. The first few lines in the `txt` files look like this:

```
STATIONS_ID;MESS_DATUM;  QN;PP_10;TT_10;TM5_10;RF_10;TD_10;eor
          3;199912312300;    1;  997.3;   4.1;   3.6;  87.0;   2.1;eor
          3;199912312310;    1;  997.3;   4.1;   3.6;  86.0;   2.0;eor
          3;199912312320;    1;  997.3;   4.2;   3.7;  87.0;   2.2;eor
```

So there are a few things to note: 

* The field delimiter character is a semi-colon (`;`)
* `STATIONS_ID` lacks the left-padded zeros.
* `MESS_DATUM` has the date format `yyyyMMddHHmm`.
* `PP_10`, `TT_10`, `TM5_10`, `RF_10`, `TD_10` are given in an invariant culture.
* `PP_10`, `TT_10`, `TM5_10`, `RF_10`, `TD_10` represent missing values with `-999`.

And we can see in the files, that a simple `string.Split` will do. We don't need to take dependencies on CSV parsers.

### Understanding the Dependencies ###

The library takes a dependency on `FluentFTP` and `Microsoft.Data.SqlClient`.

Clearly we need to download the recent and historical data from the DWD FTP Server. .NET Framework 
came with an `FtpWebRequest` for working with an FTP Server, but these classes have been deprecated 
in .NET 6 and shouldn't be used anymore. `FluentFTP` is a great library and used in the application.

To communicate with the database we use ADO.NET. The .NET Framework always came with the `System.Data.SqlClient` 
namespace, which implemented the ADO.NET for SQL Server. .NET Core also comes with it, but I strongly suggest to 
use the modern `Microsoft.Data.SqlClient` instead.

### A .NET Console Application for Data Import ###

The .NET Console Application puts all pieces together. It uses both dependencies we have to download 
the files from the FTP Server and insert them to the SQL Server database. To limit the number of 
measurements, we are only importing the historical data of Septembers.

I think the implementation contains some nice code snippets for handling transient SQL errors and 
how to avoid using a `DataTable` for sending the Table-Valued Parameters to SQL Server.

So here it is, I commented the interesting parts.

```csharp
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using FluentFTP;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System.Data;
using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;

namespace DwdAnalysis
{
    public class Program
    {
        /// <summary>
        /// Holds the Station Data.
        /// </summary>
        private record Station(
            string? StationID,
            DateTime? DatumVon,
            DateTime? DatumBis,
            float? Stationshoehe,
            float? GeoBreite,
            float? GeoLaenge,
            string? Stationsname,
            string? Bundesland
        );

        /// <summary>
        /// Holds the Weather Measurements.
        /// </summary>
        private record Messwert(
            string StationID,
            DateTime? MessDatum,
            int QN,
            float? PP_10,
            float? TT_10,
            float? TM5_10,
            float? RF_10,
            float? TD_10
        );

        /// <summary>
        /// DWD Database to store the Measurements at.
        /// </summary>
        private const string ConnectionString = @"Data Source=BYTEFISH\SQLEXPRESS;Integrated Security=true;Initial Catalog=DWD;TrustServerCertificate=Yes";

        /// <summary>
        /// Data Directory to store and read from TXT and zipped CSV files.
        /// </summary>
        private const string DataDirectory = @"C:\Users\philipp\Datasets\DWD\";

        static async Task Main(string[] args)
        {
            // Clean up the Database
            await ExecuteNonQueryAsync(ConnectionString, @"DROP PROCEDURE IF EXISTS [dbo].[usp_InsertStation];", default);
            await ExecuteNonQueryAsync(ConnectionString, @"DROP PROCEDURE IF EXISTS [dbo].[usp_InsertMesswert];", default);

            await ExecuteNonQueryAsync(ConnectionString, @"DROP TYPE IF EXISTS [dbo].[udt_StationType];", default);
            await ExecuteNonQueryAsync(ConnectionString, @"DROP TYPE IF EXISTS [dbo].[udt_MesswertType];", default);

            await ExecuteNonQueryAsync(ConnectionString, @"DROP TABLE IF EXISTS [dbo].[Station];", default);
            await ExecuteNonQueryAsync(ConnectionString, @"DROP TABLE IF EXISTS [dbo].[Messwert];", default);

            // Create Tables
            await ExecuteNonQueryAsync(ConnectionString, @"
                CREATE TABLE [dbo].[Station](
                    [StationID]         [nchar](5) NOT NULL,
                    [DatumVon]          [datetime2](7) NULL,
                    [DatumBis]          [datetime2](7) NULL,
                    [Stationshoehe]     [real] NULL,
                    [GeoBreite]         [real] NULL,
                    [GeoLaenge]         [real] NULL,
                    [Stationsname]      [nvarchar](255) NULL,
                    [Bundesland]        [nvarchar](255) NULL,
                    CONSTRAINT [PK_Station] PRIMARY KEY CLUSTERED 
                    (
                        [StationID] ASC
                    )
                ) ON [PRIMARY]", default);

            await ExecuteNonQueryAsync(ConnectionString, @"
                CREATE TABLE [dbo].[Messwert](
                    [StationID]     [nchar](5) NOT NULL,
                    [MessDatum]     [datetime2](7) NOT NULL,
                    [QN]            [int] NULL,
                    [PP_10]         [real] NULL,
                    [TT_10]         [real] NULL,
                    [TM5_10]        [real] NULL,
                    [RF_10]         [real] NULL,
                    [TD_10]         [real] NULL
                ) ON [PRIMARY]", default);

            // Create TVP Types
            await ExecuteNonQueryAsync(ConnectionString, @"                
                CREATE TYPE [dbo].[udt_StationType] AS TABLE (
                    [StationID]         [nchar](5),
                    [DatumVon]          [datetime2](7),
                    [DatumBis]          [datetime2](7),
                    [Stationshoehe]     [real],
                    [GeoBreite]         [real],
                    [GeoLaenge]         [real],
                    [Stationsname]      [nvarchar](255),
                    [Bundesland]        [nvarchar](255)
                );", default);

            await ExecuteNonQueryAsync(ConnectionString, @"
                CREATE TYPE [dbo].[udt_MesswertType] AS TABLE (
                    [StationID]     [nchar](5),
                    [MessDatum]     [datetime2](7),
                    [QN]            [int],
                    [PP_10]         [real],
                    [TT_10]         [real],
                    [TM5_10]        [real],
                    [RF_10]         [real],
                    [TD_10]         [real]
                );", default);

            // Create Stored Procedures
            await ExecuteNonQueryAsync(ConnectionString, @"
                 CREATE PROCEDURE [dbo].[usp_InsertStation]
                    @Entities [dbo].[udt_StationType] ReadOnly
                 AS
                 BEGIN
    
                     SET NOCOUNT ON;

                    INSERT INTO [dbo].[Station](StationID, DatumVon, DatumBis, Stationshoehe, GeoBreite, GeoLaenge, Stationsname, Bundesland)
                    SELECT StationID, DatumVon, DatumBis, Stationshoehe, GeoBreite, GeoLaenge, Stationsname, Bundesland
                    FROM @Entities;

                 END", default);

            await ExecuteNonQueryAsync(ConnectionString, @"
                CREATE PROCEDURE [dbo].[usp_InsertMesswert]
                    @Entities [dbo].[udt_MesswertType] ReadOnly
                AS
                BEGIN
    
                    SET NOCOUNT ON;

                    INSERT INTO [dbo].[Messwert](StationID, MessDatum, QN, PP_10, TT_10, TM5_10, RF_10, TD_10)
                    SELECT StationID, MessDatum, QN, PP_10, TT_10, TM5_10, RF_10, TD_10
                    FROM @Entities;

                END", default);

            // If the Data Directory is empty, then download all data files
            if (Directory.GetFiles(DataDirectory, "*.*").Length == 0)
            {
                await DownloadFilesInDirectoryAsync("climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical", DataDirectory, default);
                await DownloadFilesInDirectoryAsync("climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/recent", DataDirectory, default);
            }

            // The File with the DWD Station data.
            var stationTextFilePath = Path.Join(DataDirectory, "zehn_min_tu_Beschreibung_Stationen.txt");

            // Extract all Stations from the text file.
            var stations = GetStationsFromTextFile(stationTextFilePath);

            // Insert stations in batches:
            foreach (var batch in stations.Chunk(80000))
            {
                await WriteAsync(ConnectionString, batch, default);
            }

            // Get a list of all Zip Files to process:
            var zipFilesToProcess = Directory.GetFiles(DataDirectory, "*.zip");

            // Parallel Inserts
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = 5
            };

            await Parallel.ForEachAsync(zipFilesToProcess, parallelOptions, async (zipFileToProcess, cancellationToken) =>
            {
                // Extracts the Measurements from the Zip File:
                var messwerte = GetMesswerteFromZipFile(zipFileToProcess)
                    // Only use September Measurements
                    .Where(x => x.MessDatum?.Month == 9);

                // Inserts all Measurements extracted from the Zip File:
                foreach (var batch in messwerte.Chunk(80000))
                {
                    //
                    // I never trust data, and you shouldn't either. There are probably duplicates 
                    // within a batch. This would cause our MERGE to crash. Don't do this. So we
                    // group the data by (StationID, MessDatum) and select only the first value.
                    //
                    // This is "silent", can lead to hard to identify bugs and probably not
                    // what you want. The alternative is to fail hard here...
                    //
                    var data = batch
                        .GroupBy(x => new { x.StationID, x.MessDatum })
                        .Select(x => x.First());

                    await WriteAsync(ConnectionString, data, cancellationToken);
                }
            });
        }

        private static IEnumerable<Station> GetStationsFromTextFile(string stationsTextFilePath)
        {

            // Regular Expression to use for extracting stations from the fixed-width text file
            var regExpStation = new Regex("(?<stations_id>.{5})\\s(?<von_datum>.{8})\\s(?<bis_datum>.{8})\\s(?<stationshoehe>.{14})\\s(?<geo_breite>.{11})\\s(?<geo_laenge>.{9})\\s(?<stationsname>.{40})\\s(?<bundesland>.+)$", RegexOptions.Compiled);

            // Read all lines and extract the data into Station records
            return File.ReadLines(stationsTextFilePath, encoding: Encoding.Latin1)
                // Skip the Header
                .Skip(2)
                // Skip empty lines
                .Where(line => !string.IsNullOrWhiteSpace(line))
                // Apply the Regular Expression
                .Select(line => regExpStation.Match(line))
                // Extract all the matches as string values
                .Select(match => new
                {
                    StationID = match.Groups["stations_id"]?.Value?.Trim()!,
                    DatumVon = match.Groups["von_datum"]?.Value?.Trim()!,
                    DatumBis = match.Groups["bis_datum"]?.Value?.Trim()!,
                    Stationshoehe = match.Groups["stationshoehe"]?.Value?.Trim(),
                    GeoBreite = match.Groups["geo_breite"]?.Value?.Trim(),
                    GeoLaenge = match.Groups["geo_laenge"]?.Value?.Trim(),
                    Stationsname = match.Groups["stationsname"]?.Value?.Trim(),
                    Bundesland = match.Groups["bundesland"]?.Value?.Trim(),
                })
                // Now build strongly-typed records:
                .Select(columns => new Station
                (
                    StationID: columns.StationID.PadLeft(5, '0'),
                    DatumVon: DateTime.ParseExact(columns.DatumVon, "yyyyMMdd", null),
                    DatumBis: DateTime.ParseExact(columns.DatumBis, "yyyyMMdd", null),
                    Stationshoehe: string.IsNullOrWhiteSpace(columns.Stationshoehe) ? default(float?) : float.Parse(columns.Stationshoehe, CultureInfo.InvariantCulture),
                    GeoBreite: string.IsNullOrWhiteSpace(columns.GeoBreite) ? default(float?) : float.Parse(columns.GeoBreite, CultureInfo.InvariantCulture),
                    GeoLaenge: string.IsNullOrWhiteSpace(columns.GeoLaenge) ? default(float?) : float.Parse(columns.GeoLaenge, CultureInfo.InvariantCulture),
                    Stationsname: string.IsNullOrWhiteSpace(columns.Stationsname) ? default : columns.Stationsname,
                    Bundesland: string.IsNullOrWhiteSpace(columns.Bundesland) ? default : columns.Bundesland
                ));
        }

        private static IEnumerable<Messwert> GetMesswerteFromZipFile(string zipFilePath)
        {
            // Reads the lines from the Zip File an transforms them to a Messwert:
            return ReadLinesFromZipFile(zipFilePath)
                // Skip the Header:
                .Skip(1)
                // Skip empty lines:
                .Where(line => !string.IsNullOrWhiteSpace(line))
                // This isn't quoted CSV. A split will do:
                .Select(line => line.Split(";", StringSplitOptions.TrimEntries))
                // We need at least 8 fields
                .Where(fields => fields.Length >= 8)
                // Now transform them to Messwert:
                .Select(fields => new Messwert(
                    StationID: fields[0].PadLeft(5, '0'),
                    MessDatum: DateTime.ParseExact(fields[1], "yyyyMMddHHmm", null),
                    QN: int.Parse(fields[2], CultureInfo.InvariantCulture),
                    PP_10: IsValidMeasurement(fields[3]) ? float.Parse(fields[3], CultureInfo.InvariantCulture) : null,
                    TT_10: IsValidMeasurement(fields[4]) ? float.Parse(fields[4], CultureInfo.InvariantCulture) : null,
                    TM5_10: IsValidMeasurement(fields[5]) ? float.Parse(fields[5], CultureInfo.InvariantCulture) : null,
                    RF_10: IsValidMeasurement(fields[6]) ? float.Parse(fields[6], CultureInfo.InvariantCulture) : null,
                    TD_10: IsValidMeasurement(fields[7]) ? float.Parse(fields[7], CultureInfo.InvariantCulture) : null));
        }

        private static bool IsValidMeasurement(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            if (string.Equals(value, "-999", StringComparison.InvariantCultureIgnoreCase))
            {
                return false;
            }

            return true;
        }

        private static IEnumerable<string> ReadLinesFromZipFile(string zipFilePath)
        {
            using (ZipArchive zipArchive = ZipFile.OpenRead(zipFilePath))
            {
                var zipFileEntry = zipArchive.Entries[0];

                using (var zipFileStream = zipFileEntry.Open())
                {
                    using (StreamReader reader = new StreamReader(zipFileStream))
                    {
                        string? line = null;

                        while ((line = reader.ReadLine()) != null)
                        {
                            yield return line;
                        }
                    }
                }
            }
        }

        private static async Task WriteAsync(string connectionString, IEnumerable<Station> stations, CancellationToken cancellationToken)
        {
            var retryLogicProvider = GetExponentialBackoffProvider();

            using (var conn = new SqlConnection(connectionString))
            {
                // Open the Connection:
                await conn.OpenAsync();

                // Execute the Batch Write Command:
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    // Build the Stored Procedure Command:
                    cmd.CommandText = "[dbo].[usp_InsertStation]";
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.RetryLogicProvider = retryLogicProvider;

                    // Create the TVP:
                    SqlParameter parameter = new SqlParameter();

                    parameter.ParameterName = "@Entities";
                    parameter.SqlDbType = SqlDbType.Structured;
                    parameter.TypeName = "[dbo].[udt_StationType]";
                    parameter.Value = ToSqlDataRecords(stations);

                    // Add it as a Parameter:
                    cmd.Parameters.Add(parameter);

                    // And execute it:
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        private static IEnumerable<SqlDataRecord> ToSqlDataRecords(IEnumerable<Station> stations)
        {
            // Construct the Data Record with the MetaData:
            SqlDataRecord sdr = new SqlDataRecord(
                new SqlMetaData("StationID", SqlDbType.NVarChar, 5),
                new SqlMetaData("DatumVon", SqlDbType.DateTime2),
                new SqlMetaData("DatumBis", SqlDbType.DateTime2),
                new SqlMetaData("Stationshoehe", SqlDbType.Real),
                new SqlMetaData("GeoBreite", SqlDbType.Real),
                new SqlMetaData("GeoLaenge", SqlDbType.Real),
                new SqlMetaData("Stationsname", SqlDbType.NVarChar, 255),
                new SqlMetaData("Bundesland", SqlDbType.NVarChar, 255));

            // Now yield the Measurements in the Data Record:
            foreach (var station in stations)
            {
                sdr.SetString(0, station.StationID);
                SetNullableDateTime(sdr, 1, station.DatumVon);
                SetNullableDateTime(sdr, 2, station.DatumBis);
                SetNullableFloat(sdr, 3, station.Stationshoehe);
                SetNullableFloat(sdr, 4, station.GeoBreite);
                SetNullableFloat(sdr, 5, station.GeoLaenge);
                sdr.SetString(6, station.Stationsname);
                sdr.SetString(7, station.Bundesland);

                yield return sdr;
            }
        }

        private static async Task WriteAsync(string connectionString, IEnumerable<Messwert> messwerte, CancellationToken cancellationToken)
        {
            var retryLogicProvider = GetExponentialBackoffProvider();

            using (var conn = new SqlConnection(connectionString))
            {
                // Open the Connection:
                await conn.OpenAsync();

                // Execute the Batch Write Command:
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    // Build the Stored Procedure Command:
                    cmd.CommandText = "[dbo].[usp_InsertMesswert]";
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.RetryLogicProvider = retryLogicProvider;

                    // Create the TVP:
                    SqlParameter parameter = new SqlParameter();

                    parameter.ParameterName = "@Entities";
                    parameter.SqlDbType = SqlDbType.Structured;
                    parameter.TypeName = "[dbo].[udt_MesswertType]";
                    parameter.Value = ToSqlDataRecords(messwerte);

                    // Add it as a Parameter:
                    cmd.Parameters.Add(parameter);

                    // And execute it:
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static IEnumerable<SqlDataRecord> ToSqlDataRecords(IEnumerable<Messwert> stations)
        {
            // Construct the Data Record with the MetaData:
            SqlDataRecord sdr = new SqlDataRecord(
                new SqlMetaData("StationID", SqlDbType.NVarChar, 5),
                new SqlMetaData("MessDatum", SqlDbType.DateTime2),
                new SqlMetaData("QN", SqlDbType.Int),
                new SqlMetaData("PP_10", SqlDbType.Real),
                new SqlMetaData("TT_10", SqlDbType.Real),
                new SqlMetaData("TM5_10", SqlDbType.Real),
                new SqlMetaData("RF_10", SqlDbType.Real),
                new SqlMetaData("TD_10", SqlDbType.Real));

            // Now yield the Measurements in the Data Record:
            foreach (var station in stations)
            {
                sdr.SetString(0, station.StationID);
                SetNullableDateTime(sdr, 1, station.MessDatum);
                sdr.SetInt32(2, station.QN);
                SetNullableFloat(sdr, 3, station.PP_10);
                SetNullableFloat(sdr, 4, station.TT_10);
                SetNullableFloat(sdr, 5, station.TM5_10);
                SetNullableFloat(sdr, 6, station.RF_10);
                SetNullableFloat(sdr, 7, station.TD_10);

                yield return sdr;
            }
        }

        static async Task ExecuteNonQueryAsync(string connectionString, string sql, CancellationToken cancellationToken)
        {
            var retryLogicProvider = GetExponentialBackoffProvider();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                await sqlConnection.OpenAsync(cancellationToken);

                using (var cmd = sqlConnection.CreateCommand())
                {
                    cmd.RetryLogicProvider = retryLogicProvider;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = sql;

                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        static async Task DownloadFilesInDirectoryAsync(string remoteDirectory, string targetDirectory, CancellationToken cancellationToken)
        {
            using (var conn = new AsyncFtpClient("ftp://opendata.dwd.de/"))
            {
                await conn.Connect(cancellationToken);

                var ftpListItems = await conn.GetListing(remoteDirectory, token: cancellationToken);

                var ftpFileItems = ftpListItems
                    .Where(x => x.Type == FtpObjectType.File)
                    .Select(x => x.FullName);

                await conn.DownloadFiles(
                    localDir: targetDirectory,
                    remotePaths: ftpFileItems,
                    existsMode: FtpLocalExists.Overwrite,
                    token: cancellationToken);
            }
        }

        /// <summary>
        /// As a gentle reminder, the following list of transient errors are handled by the RetryLogic if the transient errors are <see cref="null"/>:
        /// 
        ///     1204,   // The instance of the SQL Server Database Engine cannot obtain a LOCK resource at this time. Rerun your statement when there are fewer active users. Ask the database administrator to check the lock and memory configuration for this instance, or to check for long-running transactions.
        ///     1205,   // Transaction (Process ID) was deadlocked on resources with another process and has been chosen as the deadlock victim. Rerun the transaction
        ///     1222,   // Lock request time out period exceeded.
        ///     49918,  // Cannot process request. Not enough resources to process request.
        ///     49919,  // Cannot process create or update request. Too many create or update operations in progress for subscription "%ld".
        ///     49920,  // Cannot process request. Too many operations in progress for subscription "%ld".
        ///     4060,   // Cannot open database "%.*ls" requested by the login. The login failed.
        ///     4221,   // Login to read-secondary failed due to long wait on 'HADR_DATABASE_WAIT_FOR_TRANSITION_TO_VERSIONING'. The replica is not available for login because row versions are missing for transactions that were in-flight when the replica was recycled. The issue can be resolved by rolling back or committing the active transactions on the primary replica. Occurrences of this condition can be minimized by avoiding long write transactions on the primary.
        ///     40143,  // The service has encountered an error processing your request. Please try again.
        ///     40613,  // Database '%.*ls' on server '%.*ls' is not currently available. Please retry the connection later. If the problem persists, contact customer support, and provide them the session tracing ID of '%.*ls'.
        ///     40501,  // The service is currently busy. Retry the request after 10 seconds. Incident ID: %ls. Code: %d.
        ///     40540,  // The service has encountered an error processing your request. Please try again.
        ///     40197,  // The service has encountered an error processing your request. Please try again. Error code %d.
        ///     42108,  // Can not connect to the SQL pool since it is paused. Please resume the SQL pool and try again.
        ///     42109,  // The SQL pool is warming up. Please try again.
        ///     10929,  // Resource ID: %d. The %s minimum guarantee is %d, maximum limit is %d and the current usage for the database is %d. However, the server is currently too busy to support requests greater than %d for this database. For more information, see http://go.microsoft.com/fwlink/?LinkId=267637. Otherwise, please try again later.
        ///     10928,  // Resource ID: %d. The %s limit for the database is %d and has been reached. For more information, see http://go.microsoft.com/fwlink/?LinkId=267637.
        ///     10060,  // An error has occurred while establishing a connection to the server. When connecting to SQL Server, this failure may be caused by the fact that under the default settings SQL Server does not allow remote connections. (provider: TCP Provider, error: 0 - A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond.) (Microsoft SQL Server, Error: 10060)
        ///     997,    // A connection was successfully established with the server, but then an error occurred during the login process. (provider: Named Pipes Provider, error: 0 - Overlapped I/O operation is in progress)
        ///     233     // A connection was successfully established with the server, but then an error occurred during the login process. (provider: Shared Memory Provider, error: 0 - No process is on the other end of the pipe.) (Microsoft SQL Server, Error: 233)
        /// </summary>
        private static SqlRetryLogicBaseProvider GetExponentialBackoffProvider(int numberOfTries = 5, int deltaTimeInSeconds = 1, int maxTimeIntervalInSeconds = 20, IEnumerable<int>? transientErrors = null)
        {
            // Define the retry logic parameters
            var options = new SqlRetryLogicOption()
            {
                // Tries 5 times before throwing an exception
                NumberOfTries = numberOfTries,
                // Preferred gap time to delay before retry
                DeltaTime = TimeSpan.FromSeconds(deltaTimeInSeconds),
                // Maximum gap time for each delay time before retry
                MaxTimeInterval = TimeSpan.FromSeconds(maxTimeIntervalInSeconds),
                // List of Transient Errors to handle
                TransientErrors = transientErrors
            };

            return SqlConfigurableRetryFactory.CreateExponentialRetryProvider(options);
        }

        private static void SetNullableFloat(SqlDataRecord sqlDataRecord, int ordinal, float? value)
        {
            if (value.HasValue)
            {
                sqlDataRecord.SetFloat(ordinal, value.Value);
            }
            else
            {
                sqlDataRecord.SetDBNull(ordinal);
            }
        }

        /// <summary>
        /// Sets the given DateTime value, or null if none is given.
        /// </summary>
        /// <param name="sqlDataRecord">SqlDataRecord to set value for</param>
        /// <param name="ordinal">Ordinal Number</param>
        /// <param name="value">float value to set</param>
        private static void SetNullableDateTime(SqlDataRecord sqlDataRecord, int ordinal, DateTime? value)
        {
            if (value.HasValue)
            {
                sqlDataRecord.SetDateTime(ordinal, value.Value);
            }
            else
            {
                sqlDataRecord.SetDBNull(ordinal);
            }
        }
    }
}
```


## Conclusion ##

And that's it. I hoped you enjoyed reading the article and learnt something new about Open Data, SQL Server or .NET!
