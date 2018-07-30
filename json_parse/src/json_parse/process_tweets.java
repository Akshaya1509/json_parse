package json_parse;

import org.json.JSONObject;
import java.sql.*;
import java.util.Date;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;




public class process_tweets {
	
	void handleactivity () throws Exception
		{
		
		final String FILENAME = "/home/akshaya/eclipse/tw_check";
		final String ccmap = "/home/akshaya/eclipse/2015_Q4_2_letter_country_codes.csv";
		String line="";
		BufferedReader br = null;
		FileReader fr = null;
	
		try {
			System.out.println("hiiii");

			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);

			String sCurrentLine;
			br = new BufferedReader(new FileReader(FILENAME));
			
			while ((sCurrentLine = br.readLine()) != null) {
				
				line += sCurrentLine;
			
			JSONObject obj = new JSONObject(line);
			
			//db connectivity
			try{  
				Class.forName("com.mysql.jdbc.Driver"); 
				//here twitter is database name, root is username and password 
				Connection con=DriverManager.getConnection(  
				 "jdbc:mysql://localhost:3306/twitter","root","root");  
				
				String query= "insert into gnip_activities (profile_id,fan_id,fan_relationship,create_time,fanname,fanhandle,fancreationtime,tweet,tweetid,timezone,interactionid,status,matchtext,matchlocation,lang,gender,sentiment,tweet_source,last_src,entity_uid,polarity,subjectivity,country_code,full_location,map_market_id,gnip_act_type,gnip_act_verb,err_code) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				PreparedStatement preparedStmt = con.prepareStatement(query);
				Statement stmt = con.createStatement();
				
				
				
				//parsing
				
				String fan,twid,ct,fct,fan_id,cc,field1,field2=null;
				String category,oprofile_id,lc_profileid,entity_uid,search_str,entity_name,sampling,kw_exclude,ht_exclude,en_exclude;
				category = oprofile_id = lc_profileid = entity_uid = search_str = entity_name = sampling = kw_exclude = ht_exclude =en_exclude= null;
				String profile_id,fan_relationship,create_time,fanname,fancreationtime,fanhandle,tweet,tweetid,timezone,interactionid,status,matchtext,matchlocation,lang,gender,sentiment,tweet_source,last_src,polarity,subjectivity,country_code,full_location,map_market_id,gnip_act_type,gnip_act_verb,err_code;
				profile_id = fan_relationship = status = matchtext = matchlocation = country_code = polarity = subjectivity  = map_market_id =interactionid= sentiment =cc= timezone = null;
				
				JSONObject actor = new JSONObject();
				JSONObject generator = new JSONObject();	
				JSONObject object = new JSONObject();
				JSONObject location = new JSONObject();
				JSONObject loc = new JSONObject();
				
				actor = obj.getJSONObject("actor");
				generator = obj.getJSONObject("generator");
				object = obj.getJSONObject("object");
				location = actor.getJSONObject("location");
				
				//if location is present
				if (obj.has("location")){
					loc = obj.getJSONObject("location");
					if (loc.has("twitter_country_code"))
					    country_code = loc.getString("twitter_country_code");
					FileReader fr1 = new FileReader(ccmap);
					BufferedReader br1 = new BufferedReader(fr1);
					
					while ((cc = br1.readLine()) != null){
						String[] f = cc.split(",");
						if (f.length >= 2){
							field1 = f[0];
							field2 = f[1];
							if (field2.equals(country_code)){
								map_market_id = field1;
							}// assigning map_market_id
						} // if csv has both the columns
							
					} //while 
					
					br1.close();
					fr1.close();	
				} // if location is present
				if (map_market_id == null)
					map_market_id  ="0";
				
				
				
				fan = actor.getString("id");
				String[] part1 = fan.split(":");
				fan_id = part1[2];
				
				//create_time
				ct = object.getString("postedTime");
		        java.text.SimpleDateFormat parser = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
		        Date date1 = parser.parse(ct);
		        java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		        create_time = formatter.format(date1); 
				
				fanname = actor.getString("displayName");
				fanhandle = actor.getString("preferredUsername");
				
				//fancreationtime
				fct = actor.getString("postedTime");
				java.text.SimpleDateFormat parser1 = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
		        Date date = parser1.parse(fct);
		        java.text.SimpleDateFormat formatter1 = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		        fancreationtime = formatter1.format(date);
				
				
				tweet = obj.getString("body");
				
				twid = obj.getString("id");
				String[] part2 = twid.split(":");
				tweetid = part2[2];
				
				if (actor.has("twitterTimeZone") && actor.get("twitterTimeZone").toString() != "null")
				timezone = actor.getString("twitterTimeZone");
				
				gender = "?";
				tweet_source = "G";
				lang = obj.getString("twitter_lang");
				last_src = generator.getString("displayName");
				
				
				full_location = location.getString("displayName");
				
				gnip_act_type=obj.getString("objectType");
				gnip_act_verb = obj.getString("verb");
				err_code = "OK";
				
				//determining if the tweet is a post or share
				if(obj.getString("verb").equals("post"))
						{
						
						}
				else if(obj.getString("verb").equals("share"))
						{
					
						}
				String query2= "SELECT vc.entity_uid,vc.category,je.profile_id,je.entity_name,sampling,COALESCE(je.search_str, '') as search_str,COALESCE(te.keyword_exclude, 0) as keyword_exclude,COALESCE(te.hashtag_exclude, 0) as hashtag_exclude,COALESCE(te.entityname_exclude, 0) as entityname_exclude FROM twitter.job_entities je Left Join twitter.tw_exclude_eids te On je.profile_id = te.profile_id INNER JOIN twitter.validation_catalog vc ON vc.twitter_id = je.profile_id WHERE streamable = 1 ORDER BY sampling DESC";
				ResultSet rs = stmt.executeQuery(query2);
				//regex
				String pattern  = "^7000";
				Pattern r = Pattern.compile(pattern);
				HashMap<String, HashMap<String, String>> in_profile2uuid = new HashMap<String, HashMap<String, String>>();
				HashMap<String, String> lc = new HashMap<String, String>();
				
				
				String link = obj.getString("link");
				
				JSONObject inReplyto = new JSONObject();
				
				while(rs.next()) 
				{
				category = rs.getString("category").toUpperCase();
				oprofile_id = rs.getString("profile_id");
				lc_profileid = rs.getString("profile_id").toLowerCase();
				entity_uid = rs.getString("entity_uid");
				search_str = rs.getString("search_str");
				sampling = rs.getString("sampling");
				kw_exclude = rs.getString("keyword_exclude");
				ht_exclude = rs.getString("hashtag_exclude");
				en_exclude = rs.getString("entityname_exclude");
				Matcher m = r.matcher(lc_profileid);
				if (!(m.find()))
					{	
					lc.put("pid", lc_profileid);
					lc.put("id", entity_uid);
					lc.put("rel", "tweeters_retweets");
					in_profile2uuid.put(lc_profileid, lc);
					}
				if (fanname.equals(lc_profileid))
					{
					//fan_relationship
					if ((kw_exclude.equals("1")) && ht_exclude.equals("1"))
						{
						fan_relationship = "tweets_mentions";
						}
					else if (kw_exclude.equals("1"))
						{
						fan_relationship = "tweets_hashtags";
						}
					else if ((obj.has("inReplyTo")))
						{
						inReplyto = obj.getJSONObject("inReplyTo");
						if (inReplyto.has("link"))
						fan_relationship = "tweeters_replies";
						}
					else
						fan_relationship = "tweeters_retweets";

					}
				
				}
			
				
				
				// printing all the values
				/*
				
				System.out.println("fan_id\t\t:\t"+fan_id);
				System.out.println("create_time\t:\t"+create_time);
				System.out.println("fan_name\t:\t"+fanname);
				System.out.println("fan_handle\t:\t"+fanhandle);
				System.out.println("fan_creationtime:\t"+fancreationtime);
				System.out.println("tweet\t\t:\t"+tweet);
				System.out.println("tweetid\t\t:\t"+tweetid);
				System.out.println("timezone\t:\t"+timezone);
				System.out.println("interactionid\t:\t"+interactionid);
				System.out.println("lang\t\t:\t"+lang);
				System.out.println("gender\t\t:\t"+gender);
				System.out.println("sentiment\t:\t"+sentiment);
				System.out.println("tweet_source\t:\t"+tweet_source);
				System.out.println("last_src\t:\t"+last_src);
				System.out.println("country_code\t:\t"+country_code);
				System.out.println("full_location\t:\t"+full_location);
				System.out.println("map_market_id\t:\t"+map_market_id);
				System.out.println("gnip_act_type\t:\t"+gnip_act_type);
				System.out.println("gnip_act_verb\t:\t"+gnip_act_verb);
				System.out.println("err_code\t:\t"+err_code);
			    */
				// inserting into table
				
				
				
				preparedStmt.setString (1,profile_id);
				preparedStmt.setString (2,fan_id);
				preparedStmt.setString (3,fan_relationship);
				preparedStmt.setString (4,create_time);
				preparedStmt.setString (5,fanname);
				preparedStmt.setString (6,fanhandle);
				preparedStmt.setString (7,fancreationtime);
				preparedStmt.setString (8,tweet);
				preparedStmt.setString (9,tweetid);
				preparedStmt.setString (10,timezone);
				preparedStmt.setString (11,interactionid);
				preparedStmt.setString (12,status);
				preparedStmt.setString (13,matchtext);
				preparedStmt.setString (14,matchlocation);
				preparedStmt.setString (15,lang);
				preparedStmt.setString (16,gender);
				preparedStmt.setString (17,sentiment);
				preparedStmt.setString (18,tweet_source);
				preparedStmt.setString (19,last_src);
				preparedStmt.setString (20,entity_uid);
				preparedStmt.setString (21,polarity);
				preparedStmt.setString (22,subjectivity);
				preparedStmt.setString (23,country_code);
				preparedStmt.setString (24,full_location);
				preparedStmt.setString (25,map_market_id);
				preparedStmt.setString (26,gnip_act_type);
				preparedStmt.setString (27,gnip_act_verb);
				preparedStmt.setString (28,err_code);
				
				preparedStmt.executeUpdate();
				
				
				
				
				
				con.close();
				}catch(Exception e)
			     { System.out.println(e);} //try 
			line = null;
}// while
			
		} catch (IOException e) {

			e.printStackTrace();// try

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();//try

			} 
		 }
	
	}
		
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		process_tweets tw_obj = new process_tweets();
		
		tw_obj.handleactivity();
		
		} //main
	} 




