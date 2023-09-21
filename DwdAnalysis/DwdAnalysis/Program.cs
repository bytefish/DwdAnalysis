﻿// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using FluentFTP;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System.Data;
using System.Globalization;
using System.IO.Compression;
using System.Text.RegularExpressions;

namespace DwdAnalysis
{
    /// <summary>
    /// Extensions for <see cref="SqlDataRecord"/>.
    /// </summary>
    public static class SqlDataRecordExtensions
    {
        /// <summary>
        /// Sets the given float value, or null if none is given.
        /// </summary>
        /// <param name="sqlDataRecord">SqlDataRecord to set value for</param>
        /// <param name="ordinal">Ordinal Number</param>
        /// <param name="value">float value to set</param>
        public static void SetNullableFloat(this SqlDataRecord sqlDataRecord, int ordinal, float? value)
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
        public static void SetNullableDateTime(this SqlDataRecord sqlDataRecord, int ordinal, DateTime? value)
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

    public class Program
    {
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
        /// Data Directory with the TXT and zipped CSV files.
        /// </summary>
        private const string DataDirectory = @"C:\Users\philipp\Datasets\DWD\";

        static async Task Main(string[] args)
        {
            // Create Tables
            await ExecuteNonQueryAsync(ConnectionString, @"
                IF NOT EXISTS (SELECT * FROM sys.objects 
                    WHERE object_id = OBJECT_ID(N'[dbo].[Station]') AND type in (N'U'))
         
                BEGIN

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
    
                END", default);

            await ExecuteNonQueryAsync(ConnectionString, @"
                IF NOT EXISTS (SELECT * FROM sys.objects 
                    WHERE object_id = OBJECT_ID(N'[dbo].[Messwert]') AND type in (N'U'))
         
                BEGIN

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

                END", default);

            // Create TVP Types
            await ExecuteNonQueryAsync(ConnectionString, @"
                IF NOT EXISTS (SELECT * FROM   [sys].[table_types]
                    WHERE  user_type_id = Type_id(N'[dbo].[udt_StationType]'))
                     
                BEGIN
                
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
                
                END", default);

            await ExecuteNonQueryAsync(ConnectionString, @"
                IF NOT EXISTS (SELECT * FROM   [sys].[table_types]
                    WHERE  user_type_id = Type_id(N'[dbo].[udt_MesswertType]'))
         
                BEGIN

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

                END", default);

            // Truncate data
            await ExecuteNonQueryAsync(ConnectionString, "TRUNCATE TABLE [dbo].[Station]", default);
            await ExecuteNonQueryAsync(ConnectionString, "TRUNCATE TABLE [dbo].[Messwert]", default);

            // Create Indices
            await ExecuteNonQueryAsync(ConnectionString, @"CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Messwert] ON [dbo].[Messwert] WITH (DROP_EXISTING = ON)", default);
            await ExecuteNonQueryAsync(ConnectionString, @"CREATE NONCLUSTERED INDEX [IX_Messwert_StationID_MessDatum] ON [dbo].[Messwert]([StationID], [MessDatum]) WITH (DROP_EXISTING = ON)", default);

            // Create Stored Procedures
            await ExecuteNonQueryAsync(ConnectionString, @"
                 CREATE OR ALTER PROCEDURE [dbo].[usp_InsertStation]
                    @Entities [dbo].[udt_StationType] ReadOnly
                 AS
                 BEGIN
    
                     SET NOCOUNT ON;

                    INSERT INTO [dbo].[Station](StationID, DatumVon, DatumBis, Stationshoehe, GeoBreite, GeoLaenge, Stationsname, Bundesland)
                    SELECT StationID, DatumVon, DatumBis, Stationshoehe, GeoBreite, GeoLaenge, Stationsname, Bundesland
                    FROM @Entities;

                 END", default);

            await ExecuteNonQueryAsync(ConnectionString, @"
                CREATE OR ALTER PROCEDURE [dbo].[usp_InsertMesswert]
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
                await DownloadDirectoryAsync(DataDirectory, default);
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
            return File.ReadLines(stationsTextFilePath)
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
            if(string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            if(string.Equals(value, "-999", StringComparison.InvariantCultureIgnoreCase))
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

                using(var zipFileStream = zipFileEntry.Open())
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
                sdr.SetNullableDateTime(1, station.DatumVon);
                sdr.SetNullableDateTime(2, station.DatumBis);
                sdr.SetNullableFloat(3, station.Stationshoehe);
                sdr.SetNullableFloat(4, station.GeoBreite);
                sdr.SetNullableFloat(5, station.GeoLaenge);
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
                sdr.SetNullableDateTime(1, station.MessDatum);
                sdr.SetInt32(2, station.QN);
                sdr.SetNullableFloat(3, station.PP_10);
                sdr.SetNullableFloat(4, station.TT_10);
                sdr.SetNullableFloat(5, station.TM5_10);
                sdr.SetNullableFloat(6, station.RF_10);
                sdr.SetNullableFloat(7, station.TD_10);

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

        static async Task DownloadDirectoryAsync(string targetDirectory, CancellationToken cancellationToken)
        {
            using (var conn = new AsyncFtpClient("ftp://opendata.dwd.de/"))
            {
                await conn.Connect(cancellationToken);

                var ftpListItems = await conn.GetListing("climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical", token: cancellationToken);

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
    }
}
