package edu.umd.cs.hcil.twitterstreamfilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;
import org.apache.log4j.rolling.RollingFileAppender;
import org.apache.log4j.rolling.TimeBasedRollingPolicy;
import org.geojson.Feature;
import org.geojson.FeatureCollection;

// API v2
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URISyntaxException;


public class StreamFilter 
{

    public StreamFilter(){
        super();
    }

    private static int cnt = 0;

    @SuppressWarnings("unused")
    private static final String MINUTE_ROLL = ".%d{yyyy-MM-dd-HH-mm}.gz";
    private static final String HOUR_ROLL = ".%d{yyyy-MM-dd-HH}.gz";
    
    private static String getExtendedApiFields() throws IOException {
        String tweetFields = "tweet.fields=attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld";
        String userFields = "user.fields=created_at,description,entities,location,name,profile_image_url,protected,public_metrics,url,username,verified,withheld";
        String expansions = "expansions=author_id,referenced_tweets.id,attachments.media_keys,entities.mentions.username,geo.place_id";
        String placeFields = "place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type";
        String mediaFields = "media.fields=preview_image_url,url";

        return "?"+expansions+"&"+tweetFields+"&"+userFields+"&"+placeFields+"&"+mediaFields; 
    }
    
    private static List<String> getKeywords(String file) throws IOException {
        List<String> keywords = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = null;
            
            while ( (line = br.readLine()) != null ) 
            {
                keywords.add(line);
            }
        }
        
        return keywords;
    }

    private static List<Double[]> readGeoJson(String file) throws IOException {
        
        List<Double[]> bboxes = new ArrayList<>();
        
        try (FileReader fr = new FileReader(file)) {
            FeatureCollection featureCollection = 
                    new ObjectMapper().readValue(fr, FeatureCollection.class);
            
            for ( Feature f : featureCollection.getFeatures() ) {
                double[] box = f.getBbox();
                Double[] reboxedBox = new Double[box.length];
                for ( int i = 0; i<box.length; i++ ) {
                    reboxedBox[i] = box[i];
                }
                bboxes.add(reboxedBox);
            }
        }
        
        return bboxes;
    }

    private static String toLocationsString(final double[][] keywords) {
        final StringBuilder buf = new StringBuilder(20 * keywords.length * 2);
        for (double[] keyword : keywords) {
            if (0 != buf.length()) {
                buf.append(" ");
            }
            buf.append(keyword[0]);
            buf.append(" ");
            buf.append(keyword[1]);
        }
        return buf.toString();
    }
    
    public static void main( String[] args ) throws IOException, URISyntaxException
    {
        // Build the command line options
        Option keywordsOpt = Option.builder("k")
                .longOpt("keywords")
                .desc("File containing keywords to track (separated by newline)")
                .hasArg()
                .build();
        
        Option boundsOpt = Option.builder("b")
                .longOpt("bounds")
                .desc("GeoJson file containing bounding boxes")
                .hasArg()
                .build();
        
        Option usersOpt = Option.builder("u")
                .longOpt("users")
                .desc("File containing user IDs to track (separated by newline)")
                .hasArg()
                .build();
        
        Option helpOpt = Option.builder("h")
                .longOpt("help")
                .desc("This help message")
                .build();
        
        Options options = new Options();
        options.addOption(keywordsOpt);
        options.addOption(boundsOpt);
        options.addOption(usersOpt);
        options.addOption(helpOpt);
        
        PatternLayout layoutStandard = new PatternLayout();
        layoutStandard.setConversionPattern("[%p] %d %c %M - %m%n");

        PatternLayout layoutSimple = new PatternLayout();
        layoutSimple.setConversionPattern("%m%n");

        // Filter for the statuses: we only want INFO messages
        LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMax(Level.INFO);
        filter.setLevelMin(Level.INFO);
        filter.setAcceptOnMatch(true);
        filter.activateOptions();

        TimeBasedRollingPolicy statusesRollingPolicy = new TimeBasedRollingPolicy();
        statusesRollingPolicy.setFileNamePattern("statuses.log" + HOUR_ROLL);
        statusesRollingPolicy.activateOptions();

        RollingFileAppender statusesAppender = new RollingFileAppender();
        statusesAppender.setRollingPolicy(statusesRollingPolicy);
        statusesAppender.addFilter(filter);
        statusesAppender.setLayout(layoutSimple);
        statusesAppender.activateOptions();

        TimeBasedRollingPolicy warningsRollingPolicy = new TimeBasedRollingPolicy();
        warningsRollingPolicy.setFileNamePattern("warnings.log" + HOUR_ROLL);
        warningsRollingPolicy.activateOptions();

        RollingFileAppender warningsAppender = new RollingFileAppender();
        warningsAppender.setRollingPolicy(warningsRollingPolicy);
        warningsAppender.setThreshold(Level.WARN);
        warningsAppender.setLayout(layoutStandard);
        warningsAppender.activateOptions();

        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setThreshold(Level.WARN);
        consoleAppender.setLayout(layoutStandard);
        consoleAppender.activateOptions();

        // configures the root logger
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.removeAllAppenders();
        rootLogger.addAppender(consoleAppender);
        rootLogger.addAppender(warningsAppender);
        
        final Logger statusLogger = Logger.getLogger("edu.umd.cs.hcil.StatusLogger");
        statusLogger.setLevel(Level.INFO);
        statusLogger.addAppender(statusesAppender);

        // creates a custom logger and log messages
        final Logger logger = Logger.getLogger(StreamFilter.class);
        String keywordString = null;
        String usersString = null;
        String locationString = null;

        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            
            if ( line.hasOption("help") ) 
            {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "StreamFilter", options );
                
                System.exit(1);
            }

            // Check if we have geojson file
            if ( line.hasOption("bounds") ) 
            {
                
                String file = line.getOptionValue("bounds");
                
                try {
                    List<Double[]> locationList = readGeoJson(file);
                    double[][] locationListDouble = new double[2][2];
                    
                    Double[] fullBox = locationList.get(0);
                    locationListDouble[0] = new double[]{fullBox[0], fullBox[1]};
                    locationListDouble[1] = new double[]{fullBox[2], fullBox[3]};
                    
                    locationString = "bounding_box: [" + toLocationsString(locationListDouble) + "]";
                    System.out.println(locationString);
                    
                } catch ( IOException ioe ) {
                    System.err.printf("Error reading GeoJSON File: [%s]", file);
                    ioe.printStackTrace();

                    System.exit(1);
                }
            }
            
            // Check if we have keyword file
            if ( line.hasOption("keywords") ) 
            {
                keywordString = "(";
                String file = line.getOptionValue("keywords");
                try 
                {
                    List<String> keywordList = getKeywords(file);
                    for(String s: keywordList)
                        {
                            keywordString += s+" OR ";
                        }
                        keywordString = keywordString.substring(0, keywordString.length() - 4) + ")";
                } 
                catch ( IOException ioe ) 
                {
                    System.err.printf("File [%s] Not Found!", file);
                    System.exit(1);
                }
            }
            
            // Check if we have a user file
            if ( line.hasOption("users") ) 
            {
                usersString = "(";
                String file = line.getOptionValue("users");
                try {
                    List<String> usersList = getKeywords(file);
                    for(String s: usersList)
                        {
                            usersString += "from:" + s + " OR ";
                        }
                        usersString = usersString.substring(0, usersString.length() - 4) + ")";
                } catch ( IOException ioe ) {
                    System.err.printf("File [%s] Not Found!", file);
                    System.exit(1);
                }
            }
        }
        catch( ParseException exp ) 
        {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }

        String bearerToken = System.getenv("BEARER_TOKEN");
        String query = null;

        if(keywordString == null && usersString == null && locationString == null)
        {
            connectSampledStream(bearerToken, statusLogger);
        } 
        else 
        {
            connectFilteredStream(bearerToken, keywordString, usersString, locationString, statusLogger);
        }

    }

    // Returns httpClient
    private static HttpClient getHttpClient()
    {
        HttpClient httpClient = HttpClients.custom()
            .setDefaultRequestConfig(RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD).build())
            .build();
        return httpClient;
    }

    // Method to connect to sampleStream API to stream all tweets
    private static void connectSampledStream(String bearerToken, Logger logger) throws IOException, URISyntaxException 
    {
        HttpClient httpClient = getHttpClient();
    
        URIBuilder sampledStreamURI = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream" + getExtendedApiFields());
    
        HttpGet httpGet = new HttpGet(sampledStreamURI.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
    
        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) 
        {
          BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
          String line = reader.readLine();
          while (line != null) 
          {
            System.out.println(line);
            logger.info(line);
            line = reader.readLine();
          }
        }
    }

    // Method to create hashmap of formatted keywords and users
    private static Map getRuleList(String keywordString, String usersString, String locationString) throws IOException 
    {
        Map<String, String> rules = new HashMap<String, String>();
        if(keywordString != null)
        {
            rules.put(keywordString, "tracking these keywords");
        }
        if(usersString != null)
        {
            rules.put(usersString, "tracking these users");
        }
        if(locationString != null)
        {
            rules.put(locationString, "tracking within this location");
        }
        return rules;
    }

    // Method to call filteredStream API after rules are set up 
    private static void connectFilteredStream(String bearerToken, String keywordString, String usersString, String locationString, Logger logger) throws IOException, URISyntaxException
    {
        Map<String, String> rulesNew = getRuleList(keywordString, usersString, locationString);

        setupRules(bearerToken, rulesNew);
        HttpClient httpClient = getHttpClient();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream" + getExtendedApiFields());

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) 
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
            String line = reader.readLine();
            while (line != null) 
            {
                System.out.println(line);
                line = reader.readLine();
            }
        }
    }

    // Method to setup rules by deleting old rules and creating new ones upon runtime
    private static void setupRules(String bearerToken, Map<String, String> rules) throws IOException, URISyntaxException 
    {
        List<String> existingRules = getRules(bearerToken);
        if (existingRules.size() > 0) 
        {
            deleteRules(bearerToken, existingRules);
        }
        createRules(bearerToken, rules);
    }
    
    // Method to create rules to add to filteredStream
    private static void createRules(String bearerToken, Map<String, String> rules) throws URISyntaxException, IOException 
    {
        HttpClient httpClient = getHttpClient();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) 
        {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }
    
    // Method to get existing rules 
    private static List<String> getRules(String bearerToken) throws URISyntaxException, IOException {
        List<String> rules = new ArrayList<>();
        HttpClient httpClient = getHttpClient();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("content-type", "application/json");
        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) 
        {
            JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1) 
            {
                JSONArray array = (JSONArray) json.get("data");
                for (int i = 0; i < array.length(); i++) 
                {
                    JSONObject jsonObject = (JSONObject) array.get(i);
                    rules.add(jsonObject.getString("id"));
                }
            }
        }
        return rules;
    }
    
    // Method to delete existing rules
    private static void deleteRules(String bearerToken, List<String> existingRules) throws URISyntaxException, IOException 
    {
        HttpClient httpClient = getHttpClient();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) 
        {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }
    
    private static String getFormattedString(String string, List<String> ids) {
        StringBuilder sb = new StringBuilder();
        if (ids.size() == 1) 
        {
            return String.format(string, "\"" + ids.get(0) + "\"");
        } 
        else 
        {
            for (String id : ids) 
            {
                sb.append("\"" + id + "\"" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }
    
    private static String getFormattedString(String string, Map<String, String> rules) {
        StringBuilder sb = new StringBuilder();
        if (rules.size() == 1) 
        {
            String key = rules.keySet().iterator().next();
            return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
        } 
        else 
        {
            for (Map.Entry<String, String> entry : rules.entrySet()) 
            {
                String value = entry.getKey();
                String tag = entry.getValue();
                sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }
}