using System;
using System.Collections.Generic;
using System.Net;
using Dse;

namespace automation.components.data.v1.Cassandra
{
    public interface IDSEConnectionBuilder
    {
        ICollection<IPEndPoint> ContactPoints { get; }

        PoolingOptions PoolingOptions { get; }

        SocketOptions SocketOptions { get; }

        IDSEConnectionBuilder AddContactPoint(string address);

        IDSEConnectionBuilder AddContactPoint(IPAddress address);

        IDSEConnectionBuilder AddContactPoint(IPEndPoint address);

        IDSEConnectionBuilder AddContactPoints(params string[] addresses);

        IDSEConnectionBuilder AddContactPoints(IEnumerable<string> addresses);

        IDSEConnectionBuilder AddContactPoints(params IPAddress[] addresses);

        IDSEConnectionBuilder AddContactPoints(IEnumerable<IPAddress> addresses);

        IDSEConnectionBuilder AddContactPoints(params IPEndPoint[] addresses);

        IDSEConnectionBuilder AddContactPoints(IEnumerable<IPEndPoint> addresses);

        Cluster Build();

        Configuration GetConfiguration();

        IDSEConnectionBuilder WithAddressTranslator(IAddressTranslator addressTranslator);

        IDSEConnectionBuilder WithAuthProvider(IAuthProvider authProvider);

        IDSEConnectionBuilder WithCompression(CompressionType compression);

        IDSEConnectionBuilder WithConnectionString(string connectionString);

        IDSEConnectionBuilder WithCredentials(string username, string password);

        IDSEConnectionBuilder WithCustomCompressor(IFrameCompressor compressor);

        IDSEConnectionBuilder WithDefaultKeyspace(string defaultKeyspace);

        IDSEConnectionBuilder WithLoadBalancingPolicy(ILoadBalancingPolicy policy);

        IDSEConnectionBuilder WithMaxProtocolVersion(byte version);

        IDSEConnectionBuilder WithoutRowSetBuffering();

        IDSEConnectionBuilder WithPoolingOptions(PoolingOptions value);

        IDSEConnectionBuilder WithPort(int port);

        IDSEConnectionBuilder WithQueryOptions(QueryOptions options);

        IDSEConnectionBuilder WithQueryTimeout(int queryAbortTimeout);

        IDSEConnectionBuilder WithReconnectionPolicy(IReconnectionPolicy policy);

        IDSEConnectionBuilder WithRetryPolicy(IRetryPolicy policy);

        IDSEConnectionBuilder WithSocketOptions(SocketOptions value);

        IDSEConnectionBuilder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy policy);

        IDSEConnectionBuilder WithSSL(SSLOptions sslOptions);

        IDSEConnectionBuilder WithSSL();
}

    public class DSEConnectionBuilderWrapper : IDSEConnectionBuilder
    {
        private Builder builder;

        public DSEConnectionBuilderWrapper()
        {
            this.builder = Cluster.Builder();
        }

        ICollection<IPEndPoint> IDSEConnectionBuilder.ContactPoints
        {
            get
            {
                return this.builder.ContactPoints;
            }
        }

        PoolingOptions IDSEConnectionBuilder.PoolingOptions
        {
            get
            {
                return this.builder.PoolingOptions;
            }
        }

        SocketOptions IDSEConnectionBuilder.SocketOptions
        {
            get
            {
                return this.builder.SocketOptions;
            }
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoint(string address)
        {
            this.builder = this.builder.AddContactPoint(address);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoint(IPAddress address)
        {
            this.builder = this.builder.AddContactPoint(address);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoint(IPEndPoint address)
        {
            this.builder = this.builder.AddContactPoint(address);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoints(params string[] addresses)
        {
            this.builder = this.builder.AddContactPoints(addresses);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoints(IEnumerable<string> addresses)
        {
            this.builder = this.builder.AddContactPoints(addresses);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoints(params IPAddress[] addresses)
        {
            this.builder = this.builder.AddContactPoints(addresses);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoints(IEnumerable<IPAddress> addresses)
        {
            this.builder = this.builder.AddContactPoints(addresses);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoints(params IPEndPoint[] addresses)
        {
            this.builder = this.builder.AddContactPoints(addresses);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.AddContactPoints(IEnumerable<IPEndPoint> addresses)
        {
            this.builder = this.builder.AddContactPoints(addresses);
            return this;
        }

        Cluster IDSEConnectionBuilder.Build()
        {
            return this.builder.Build();
        }

        Configuration IDSEConnectionBuilder.GetConfiguration()
        {
            return this.builder.GetConfiguration();
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithAddressTranslator(IAddressTranslator addressTranslator)
        {
            this.builder = this.builder.WithAddressTranslator(addressTranslator);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithAuthProvider(IAuthProvider authProvider)
        {
            this.builder = this.builder.WithAuthProvider(authProvider);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithCompression(CompressionType compression)
        {
            this.builder = this.builder.WithCompression(compression);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithConnectionString(string connectionString)
        {
            this.builder = this.builder.WithConnectionString(connectionString);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithCredentials(string username, string password)
        {
            this.builder = this.builder.WithCredentials(username, password);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithCustomCompressor(IFrameCompressor compressor)
        {
            this.builder = this.builder.WithCustomCompressor(compressor);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithDefaultKeyspace(string defaultKeyspace)
        {
            this.builder = this.builder.WithDefaultKeyspace(defaultKeyspace);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            this.builder = this.builder.WithLoadBalancingPolicy(policy);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithMaxProtocolVersion(byte version)
        {
            this.builder = this.builder.WithMaxProtocolVersion(version);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithoutRowSetBuffering()
        {
            this.builder = this.builder.WithoutRowSetBuffering();
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithPoolingOptions(PoolingOptions value)
        {
            this.builder = this.builder.WithPoolingOptions(value);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithPort(int port)
        {
            this.builder = this.builder.WithPort(port);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithQueryOptions(QueryOptions options)
        {
            this.builder = this.builder.WithQueryOptions(options);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithQueryTimeout(int queryAbortTimeout)
        {
            this.builder = this.builder.WithQueryTimeout(queryAbortTimeout);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithReconnectionPolicy(IReconnectionPolicy policy)
        {
            this.builder = this.builder.WithReconnectionPolicy(policy);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithRetryPolicy(IRetryPolicy policy)
        {
            this.builder = this.builder.WithRetryPolicy(policy);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithSocketOptions(SocketOptions value)
        {
            this.builder = this.builder.WithSocketOptions(value);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy policy)
        {
            this.builder = this.builder.WithSpeculativeExecutionPolicy(policy);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithSSL(SSLOptions sslOptions)
        {
            this.builder = this.builder.WithSSL(sslOptions);
            return this;
        }

        IDSEConnectionBuilder IDSEConnectionBuilder.WithSSL()
        {
            this.builder = this.builder.WithSSL();
            return this;
        }
    }
}
