﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.Producers.Partitioning
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Utils;

    /// <summary>
    /// Fetch broker info like ID, host and port from configuration.
    /// </summary>
    /// <remarks>
    /// Used when zookeeper based auto partition discovery is disabled
    /// </remarks>
    internal class ConfigBrokerPartitionInfo : IBrokerPartitionInfo
    {
        private readonly ProducerConfiguration config;
        private IDictionary<int, Broker> brokers;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigBrokerPartitionInfo"/> class.
        /// </summary>
        /// <param name="config">The config.</param>
        public ConfigBrokerPartitionInfo(ProducerConfiguration config)
        {
            Guard.NotNull(config, "config");
            this.config = config;
            this.InitializeBrokers();
        }

        /// <summary>
        /// Gets a mapping from broker ID to the host and port for all brokers
        /// </summary>
        /// <returns>
        /// Mapping from broker ID to the host and port for all brokers
        /// </returns>
        public IDictionary<int, Broker> GetAllBrokerInfo()
        {
            return this.brokers;
        }

        /// <summary>
        /// Gets a mapping from broker ID to partition IDs
        /// </summary>
        /// <param name="topic">The topic for which this information is to be returned</param>
        /// <returns>
        /// Mapping from broker ID to partition IDs
        /// </returns>
        /// <remarks>Partition ID would be allways 0</remarks>
        public SortedSet<Partition> GetBrokerPartitionInfo(string topic)
        {
            Guard.NotNullNorEmpty(topic, "topic");
            var partitions = new SortedSet<Partition>();
            foreach (var item in this.brokers)
            {
                partitions.Add(new Partition(item.Key, 0));
            }

            return partitions;
        }

        /// <summary>
        /// Gets the host and port information for the broker identified by the given broker ID
        /// </summary>
        /// <param name="brokerId">The broker ID.</param>
        /// <returns>
        /// Host and port of broker
        /// </returns>
        public Broker GetBrokerInfo(int brokerId)
        {
            return this.brokers.ContainsKey(brokerId) ? this.brokers[brokerId] : null;
        }

        /// <summary>
        /// Releasing unmanaged resources if any are used.
        /// </summary>
        /// <remarks>Do nothing</remarks>
        public void Dispose()
        {
        }

        /// <summary>
        /// Initialize list of brokers from configuration
        /// </summary>
        private void InitializeBrokers()
        {
            if (this.brokers != null)
            {
                return;
            }

            this.brokers = new Dictionary<int, Broker>();
            foreach (var item in this.config.Brokers)
            {
                this.brokers.Add(
                    item.BrokerId, 
                    new Broker(item.BrokerId, item.Host, item.Host, item.Port));
            }
        }
    }
}
