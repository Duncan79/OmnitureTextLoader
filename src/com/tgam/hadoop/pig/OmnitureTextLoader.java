package com.tgam.hadoop.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;


/**
 * A Pig custom loader for reading and parsing raw Omniture daily hit data files (hit_data.tsv).
 * @author Duncan M. Gunn (<a href="mailto:gunn.duncan@gmail.com">gunn.duncan@gmail.com</a>) based on original by Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 */
public class OmnitureTextLoader extends LoadFunc implements LoadMetadata {
	// private static final Log LOG = LogFactory.getLog(OmnitureTextLoader.class);
	
	private static final String DELIMITER = "\\t";
	// Yes, I could store this schema somewhere else rather than hard code, but in the case of Omniture's
	// files they change so infrequently that it seemed to make sense to put it in schema.txt then use
	// a tiny Ruby script to generate the hard coded string
	private static final String STRING_SCHEMA = "accept_language:chararray,browser:chararray,browser_height:chararray,browser_width:chararray,campaign:chararray,c_color:chararray,channel:chararray,click_action:chararray,click_action_type:chararray,click_context:chararray,click_context_type:chararray,click_sourceid:chararray,click_tag:chararray,code_ver:chararray,color:chararray,connection_type:chararray,cookies:chararray,country:chararray,ct_connect_type:chararray,currency:chararray,curr_factor:chararray,curr_rate:chararray,cust_hit_time_gmt:chararray,cust_visid:chararray,daily_visitor:chararray,date_time:chararray,domain:chararray,duplicated_from:chararray,duplicate_events:chararray,duplicate_purchase:chararray,evar1:chararray,evar4:chararray,evar5:chararray,evar6:chararray,evar7:chararray,evar12:chararray,evar15:chararray,evar17:chararray,evar18:chararray,evar21:chararray,evar22:chararray,evar23:chararray,evar24:chararray,evar25:chararray,evar28:chararray,evar31:chararray,evar43:chararray,evar46:chararray,evar49:chararray,evar50:chararray,event_list:chararray,exclude_hit:chararray,first_hit_pagename:chararray,first_hit_page_url:chararray,first_hit_referrer:chararray,first_hit_time_gmt:chararray,geo_city:chararray,geo_country:chararray,geo_dma:chararray,geo_region:chararray,geo_zip:chararray,hier1:chararray,hier2:chararray,hier3:chararray,hier4:chararray,hier5:chararray,hitid_high:chararray,hitid_low:chararray,hit_source:chararray,hit_time_gmt:chararray,homepage:chararray,hourly_visitor:chararray,ip:chararray,ip2:chararray,java_enabled:chararray,javascript:chararray,j_jscript:chararray,language:chararray,last_hit_time_gmt:chararray,last_purchase_num:chararray,last_purchase_time_gmt:chararray,mobile_id:chararray,monthly_visitor:chararray,mvvar1:chararray,mvvar2:chararray,mvvar3:chararray,namespace:chararray,new_visit:chararray,os:chararray,page_event:chararray,page_event_var1:chararray,page_event_var2:chararray,page_event_var3:chararray,pagename:chararray,page_type:chararray,page_url:chararray,paid_search:chararray,partner_plugins:chararray,persistent_cookie:chararray,plugins:chararray,post_browser_height:chararray,post_browser_width:chararray,post_campaign:chararray,post_channel:chararray,post_cookies:chararray,post_currency:chararray,post_cust_hit_time_gmt:chararray,post_cust_visid:chararray,post_evar1:chararray,post_evar4:chararray,post_evar5:chararray,post_evar6:chararray,post_evar7:chararray,post_evar12:chararray,post_evar15:chararray,post_evar17:chararray,post_evar18:chararray,post_evar21:chararray,post_evar22:chararray,post_evar23:chararray,post_evar24:chararray,post_evar25:chararray,post_evar28:chararray,post_evar31:chararray,post_evar43:chararray,post_evar46:chararray,post_evar49:chararray,post_evar50:chararray,post_event_list:chararray,post_hier1:chararray,post_hier2:chararray,post_hier3:chararray,post_hier4:chararray,post_hier5:chararray,post_java_enabled:chararray,post_keywords:chararray,post_mvvar1:chararray,post_mvvar2:chararray,post_mvvar3:chararray,post_page_event:chararray,post_page_event_var1:chararray,post_page_event_var2:chararray,post_page_event_var3:chararray,post_pagename:chararray,post_pagename_no_url:chararray,post_page_type:chararray,post_page_url:chararray,post_partner_plugins:chararray,post_persistent_cookie:chararray,post_product_list:chararray,post_prop3:chararray,post_prop5:chararray,post_prop6:chararray,post_prop7:chararray,post_prop9:chararray,post_prop20:chararray,post_prop21:chararray,post_prop22:chararray,post_prop23:chararray,post_prop26:chararray,post_prop29:chararray,post_prop30:chararray,post_prop33:chararray,post_prop35:chararray,post_prop36:chararray,post_prop37:chararray,post_prop59:chararray,post_prop68:chararray,post_prop73:chararray,post_purchaseid:chararray,post_referrer:chararray,post_search_engine:chararray,post_state:chararray,post_survey:chararray,post_tnt:chararray,post_transactionid:chararray,post_t_time_info:chararray,post_visid_high:chararray,post_visid_low:chararray,post_visid_type:chararray,post_zip:chararray,p_plugins:chararray,prev_page:chararray,product_list:chararray,product_merchandising:chararray,prop3:chararray,prop5:chararray,prop6:chararray,prop7:chararray,prop9:chararray,prop20:chararray,prop21:chararray,prop22:chararray,prop23:chararray,prop26:chararray,prop29:chararray,prop30:chararray,prop33:chararray,prop35:chararray,prop36:chararray,prop37:chararray,prop59:chararray,prop68:chararray,prop73:chararray,purchaseid:chararray,quarterly_visitor:chararray,ref_domain:chararray,referrer:chararray,ref_type:chararray,resolution:chararray,sampled_hit:chararray,search_engine:chararray,search_page_num:chararray,secondary_hit:chararray,service:chararray,sourceid:chararray,s_resolution:chararray,state:chararray,stats_server:chararray,tnt:chararray,tnt_post_vista:chararray,transactionid:chararray,truncated_hit:chararray,t_time_info:chararray,ua_color:chararray,ua_os:chararray,ua_pixels:chararray,user_agent:chararray,user_hash:chararray,userid:chararray,username:chararray,user_server:chararray,va_closer_detail:chararray,va_closer_id:chararray,va_finder_detail:chararray,va_finder_id:chararray,va_instance_event:chararray,va_new_engagement:chararray,visid_high:chararray,visid_low:chararray,visid_new:chararray,visid_timestamp:chararray,visid_type:chararray,visit_keywords:chararray,visit_num:chararray,visit_page_num:chararray,visit_referrer:chararray,visit_search_engine:chararray,visit_start_pagename:chararray,visit_start_page_url:chararray,visit_start_time_gmt:chararray,weekly_visitor:chararray,yearly_visitor:chararray,zip:chararray";
	private static final int FIELD_COUNT = STRING_SCHEMA.split(",").length;
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	private RecordReader reader;
	private String udfcSignature = null;
	private ResourceFieldSchema[] fields;
	
	@Override
	public void setUDFContextSignature(String signature) {
		udfcSignature = signature;
	}

	@Override
	/**
	 * Provide a new OmnitureDataFileInputFormat for RecordReading.
	 * @return a new OmnitureDataFileInputFormat()
	 */
	public InputFormat getInputFormat() throws IOException {
		return new OmnitureDataFileInputFormat();
	}
	
	@Override
	/**
	 * Sets the location of the data file for the call to this custom loader.  This is assumed to be an HDFS path
	 * and thus FileInputFormat is used.
	 */
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);	
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// LOG.info("RecordReader is of type " + reader.getClass().getName());
		this.reader = reader;
		ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(STRING_SCHEMA));
		fields = schema.getFields();
	}
	
	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
		Text value = null;
		String []values;
		
		try {
			// Read the next key-value pair from the record reader.  If it's
			// finished, return null
			if (!reader.nextKeyValue()) return null;
			
			value = (Text)reader.getCurrentValue();
			values = value.toString().split(DELIMITER, -1);
		} catch (InterruptedException ie) {
			throw new IOException(ie);
		}
				
		// Create a new Tuple optimized for the number of fields that we know we'll need
		tuple = tupleFactory.newTuple(FIELD_COUNT);
		
		for (int i = 0; i < FIELD_COUNT - 1; i++) {			
			// Optimization
			ResourceFieldSchema field = fields[i];
			
			String val = values[i];
			
			switch(field.getType()) {
			case DataType.INTEGER:
				try {
					tuple.set(i, Integer.parseInt(val));
				} catch (NumberFormatException nfe1) {
					// Throw a more descriptive message
					throw new NumberFormatException("Error while trying to parse " + val + " into an Integer for field " + field.getName() + "\n" + value.toString());
				}
				break;
			case DataType.CHARARRAY:
				tuple.set(i, val);
				break;
			case DataType.LONG:
				try {
					tuple.set(i, Long.parseLong(val));
				} catch (NumberFormatException nfe2) {
					throw new NumberFormatException("Error while trying to parse " + val + " into a Long for field " + field.getName() + "\n" + value.toString());
				}
				
				break;
			case DataType.BAG:
				if (field.getName().equals("event_list")) {
					DataBag bag = bagFactory.newDefaultBag();
					String []events = val.split(",");
					
					if (events == null) {
						tuple.set(i, null);
					} else {
						for (int j = 0; j < events.length; j++) {
							Tuple t = tupleFactory.newTuple(1);
							if (events[j] == "") {
								t.set(0, null);
							} else {
								t.set(0, events[j]);
							}
							bag.add(t);
						}
						tuple.set(i, bag);
					}					
				} else {
					throw new IOException("Can not process bags for the field " + field.getName() + ". Can only process for the event_list field.");
				}
				break;
			default:
				throw new IOException("Unexpected or unknown type in input schema (Omniture fields should be int, chararray or long): " + field.getType());
			}
		}
		
		return tuple;
	}

	@Override
	public ResourceSchema getSchema(String location, Job job) throws IOException {
		// The schema for hit_data.tsv won't change for quite sometime and when it does, this class should be updated
		
		ResourceSchema s = new ResourceSchema(Utils.getSchemaFromString(STRING_SCHEMA));
		
		// Store the schema to our UDF context on the backend (is this really necessary considering it's private static final?)
		UDFContext udfc = UDFContext.getUDFContext();
		Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
		p.setProperty("pig.omnituretextloader.schema", STRING_SCHEMA);
		
		return s;
	}
	
	@Override
	/** 
	 * Not currently used, but could later on be used to partition based on hit_time_gmt perhaps.
	 */
	public String[] getPartitionKeys(String location, Job job) throws IOException {
		// TODO: Build out partition keys based on hit_time_gmt 
		return null;
	}


	@Override
	/**
	 * Not used in this class.
	 * @return null
	 */
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	/** 
	 * Not currently used, but could later on be used to partition based on hit_time_gmt perhaps.
	 */
	public void setPartitionFilter(Expression arg0) throws IOException {
		// TODO: Build out partition keys based on hit_time_gmt
		
	}
}
