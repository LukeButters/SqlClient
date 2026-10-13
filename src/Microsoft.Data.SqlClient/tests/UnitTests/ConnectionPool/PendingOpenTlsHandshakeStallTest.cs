// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.ManagedSni;
using Octopus.TestPortForwarder;
using Serilog.Core;
using Xunit;

namespace Microsoft.Data.SqlClient.UnitTests.ConnectionPool;

/// <summary>
/// Against a real SQL Server, with one physical open paused in its TLS handshake.
/// </summary>
public class PendingOpenTlsHandshakeStallTest
{
    private static readonly string s_serverConnectionString =
        Environment.GetEnvironmentVariable("MDS_TCPConnectionString")
        ?? "Data Source=tcp:localhost,1433;Database=master;User ID=sa;Password=Password01!;TrustServerCertificate=true;";

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WhenOpeningADbConnection_AndTheTCPConnectionHangsDuringTLSHandshake_TheOpenAttemptEventuallyFails(bool handshakeTimeoutEnabled)
    {
        TdsParserStateObjectManaged.TlsHandshakeTimeoutEnabled = handshakeTimeoutEnabled;
        int connectTimeoutSeconds = 5;
        using var stall = new HandshakeStall(connectTimeoutSeconds);

        // Start with everything working.
        stall.DisableTCPStallOnNewConnections();
        using var idle = new SqlConnection(stall.ConnectionString);
        await idle.OpenAsync();

        // Break the connection on the next request.
        stall.EnableTcpStallJustBeforeTlsHandshake();
        using var hungConnection = new SqlConnection(stall.ConnectionString);
        Task hungOpen = hungConnection.OpenAsync();
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.False(hungOpen.IsCompleted, "the open should be stuck in its TLS handshake");

        // Does the connection open timeout after 10s?
        Assert.Same(hungOpen, await Task.WhenAny(hungOpen, Task.Delay(TimeSpan.FromSeconds(connectTimeoutSeconds * 2))));
        SqlException timedOut = await Assert.ThrowsAsync<SqlException>(() => hungOpen);
        Assert.Contains("timeout", timedOut.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WhenATcpConnectionHangsDuringTLSHandshake_ThePoolIsAbleToGrowAfterTheOpenTimesOut(bool handshakeTimeoutEnabled)
    {
        TdsParserStateObjectManaged.TlsHandshakeTimeoutEnabled = handshakeTimeoutEnabled;
        int connectTimeoutSeconds = 5;
        using var stall = new HandshakeStall(connectTimeoutSeconds);

        // Break the first connection the pool tries to create.
        stall.EnableTcpStallJustBeforeTlsHandshake();
        using var hungConnection = new SqlConnection(stall.ConnectionString);
        Task hungOpen = hungConnection.OpenAsync();
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.False(hungOpen.IsCompleted, "the open should be stuck in its TLS handshake");

        // The network is healthy again, so the pool only needs to be able to create a connection.
        stall.DisableTCPStallOnNewConnections();

        // Wait for the hung open to time out, plus the 5s pool blocking period that follows a failed create.
        await Task.Delay(TimeSpan.FromSeconds(connectTimeoutSeconds * 2 + 5));
        using var nextConnection = new SqlConnection(stall.ConnectionString);
        Task nextOpen = nextConnection.OpenAsync();

        // Does the next open get a connection within 2x the connect timeout?
        Assert.Same(nextOpen, await Task.WhenAny(nextOpen, Task.Delay(TimeSpan.FromSeconds(connectTimeoutSeconds * 2))));
        await nextOpen;
    }

    /// <summary>
    /// Forwards to SQL Server. While the stall is enabled, any connection that reaches its TLS handshake
    /// is paused there: it stays open but no more data flows.
    /// </summary>
    private sealed class HandshakeStall : IDisposable
    {
        private readonly PortForwarder _forwarder;
        private volatile bool _stallEnabled;

        public HandshakeStall(int connectTimeoutSeconds)
        {
            var server = new SqlConnectionStringBuilder(s_serverConnectionString);
            string[] hostAndPort = server.DataSource.Replace("tcp:", "").Split(',');
            _forwarder = new PortForwarderBuilder(
                new Uri($"tcp://{hostAndPort[0]}:{(hostAndPort.Length > 1 ? hostAndPort[1] : "1433")}"), Logger.None).Build();

            ConnectionString = new SqlConnectionStringBuilder(s_serverConnectionString)
            {
                DataSource = $"tcp:127.0.0.1,{_forwarder.ListeningPort}",
                Encrypt = SqlConnectionEncryptOption.Mandatory,
                TrustServerCertificate = true,
                Pooling = true,
                MaxPoolSize = 100,
                ConnectTimeout = connectTimeoutSeconds,
                ApplicationName = Guid.NewGuid().ToString(),
            }.ConnectionString;

            SniTcpHandle.BeforeTlsHandshake = () =>
            {
                if (_stallEnabled)
                {
                    _forwarder.PauseExistingConnections();
                }
            };
        }

        public string ConnectionString { get; }

        public void EnableTcpStallJustBeforeTlsHandshake() => _stallEnabled = true;

        public void DisableTCPStallOnNewConnections() => _stallEnabled = false;

        public void Dispose()
        {
            SniTcpHandle.BeforeTlsHandshake = null;
            TdsParserStateObjectManaged.TlsHandshakeTimeoutEnabled = true;
            _forwarder.Dispose();
            SqlConnection.ClearAllPools();
        }
    }
}
