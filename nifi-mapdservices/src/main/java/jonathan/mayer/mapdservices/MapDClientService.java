/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jonathan.mayer.mapdservices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.thrift.TException;

import com.mapd.thrift.server.MapD;
import com.mapd.thrift.server.TMapDException;

@Tags({ "example"})
@CapabilityDescription("Example ControllerService implementation of MyService.")
public class MapDClientService extends AbstractControllerService implements MapDService {

    
	public static final PropertyDescriptor MAPD_URL = new PropertyDescriptor.Builder().name("JDBC_URL")
			.displayName("MAPD_URL").description("JDBC_URL").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder().name("USER_NAME")
			.displayName("USER_NAME").description("USER_NAME").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor USER_PASSWORD = new PropertyDescriptor.Builder().name("USER_PASSWORD")
			.displayName("USER_PASSWORD").description("USER_PASSWORD").sensitive(true).required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder().name("DB_NAME")
			.displayName("DB_NAME").description("DB_NAME").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor PORT_NUMBER = new PropertyDescriptor.Builder().name("PORT_NUMBER")
			.displayName("PORT_NUMBER").description("PORT_NUMBER").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	
	private static String mapdurl;
	private static String username;
	private static String password;
	private static String dbname;
	private static int portno;
	private static MapD.Client mapdclient;
	private static String mapdsession;

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(MAPD_URL);
		props.add(USER_NAME);
		props.add(USER_PASSWORD);
		props.add(DB_NAME);
		props.add(PORT_NUMBER);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     * @throws TException 
     * @throws TMapDException 
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, TMapDException, TException {
    	mapdurl = context.getProperty(MAPD_URL).getValue();
		username = context.getProperty(USER_NAME).getValue();
		password = context.getProperty(USER_PASSWORD).getValue();
		dbname = context.getProperty(DB_NAME).getValue();
		portno =  context.getProperty(PORT_NUMBER).asInteger();
		mapdclient = MapDClientUtils.get_client(mapdurl, portno, false);
		
		mapdsession = mapdclient.connect(username, password, dbname);
    }

    @OnDisabled
    public void shutdown() {

    }

    @Override
	public MapD.Client GetMapDClient() throws ProcessException {
		// TODO Auto-generated method stub
		return mapdclient;
	}
    
//    @Override
	public String MapDSession() throws ProcessException {
		// TODO Auto-generated method stub
		return mapdsession;
	}

}
