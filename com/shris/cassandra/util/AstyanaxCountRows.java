/*******************************************************************************
 * Copyright 2012 Shris Infotech Solutions India (Pvt.) Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.shris.cassandra.util;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

/**
 * Class to count number of rows in a Cassandra column family.
 *
 * @author Kishore Kopalle (kkopalle@gmail.com)
 */
public class AstyanaxCountRows {

    private static String SEEDS = "192.168.1.159:9160";

	private final static String CLUSTER_NAME = "StressCluster";
	private final static String KEYSPACE_NAME = "StressKeyspace";
	private final static String COLUMN_FAMILY = "StressStandard";

	private static  com.netflix.astyanax.Keyspace keyspace;
	private static AstyanaxContext<Keyspace> keyspaceContext;

	    public static ColumnFamily<String, String> CF_STANDARD = ColumnFamily
	            .newColumnFamily(COLUMN_FAMILY, StringSerializer.get(),
	                    StringSerializer.get());

	public static void main(String[] args)
	{

		double nanoStart = System.nanoTime();
        keyspaceContext = new AstyanaxContext.Builder()
        .forCluster(CLUSTER_NAME)
        .forKeyspace(KEYSPACE_NAME)
        .withAstyanaxConfiguration(
                new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.NONE))
        .withConnectionPoolConfiguration(
                new ConnectionPoolConfigurationImpl(CLUSTER_NAME
                        + "_" + KEYSPACE_NAME)
                		.setMaxConnsPerHost(1000)
                		.setMaxBlockedThreadsPerHost(1000)
                        .setSocketTimeout(30000)
                        .setMaxTimeoutWhenExhausted(2000)
                        .setMaxConnsPerHost(1).setSeeds(SEEDS))
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
        .buildKeyspace(ThriftFamilyFactory.getInstance());

        keyspaceContext.start();

        keyspace = keyspaceContext.getEntity();
		try {
	        OperationResult<Rows<String, String>> rows = keyspace.prepareQuery(CF_STANDARD)
	        		.getAllRows()
	        		.setRowLimit(10000)  // This is the page size
	        		.withColumnRange(new RangeBuilder().setMaxSize(1000).build())
	        		.setExceptionCallback(new ExceptionCallback() {
	        	            @Override
	        	            public boolean onException(ConnectionException e) {
	        	            	e.printStackTrace();
	        	                return true;
	        	            }
	        		})
	        		.execute();
	        double counter = 0D;
	        for (Row<String, String> row : rows.getResult()) {
	        	counter++;
	        }
        	double timeTaken = (System.nanoTime()-nanoStart)/(1e9);
        	System.out.println("Time taken to fetch " + counter + " rows is " + "" + timeTaken + " seconds.");
		} 
		catch (ConnectionException e) {
	        		e.printStackTrace();
		}
	}
}
