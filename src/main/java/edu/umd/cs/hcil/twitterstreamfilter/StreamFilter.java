package edu.umd.cs.hcil.twitterstreamfilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import twitter4j.FilterQuery;
import twitter4j.RawStreamListener;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Hello world!
 *
 */
public class StreamFilter 
{
    
    private static int cnt = 0;

    @SuppressWarnings("unused")
    private static final String MINUTE_ROLL = ".%d{yyyy-MM-dd-HH-mm}.gz";
    private static final String HOUR_ROLL = ".%d{yyyy-MM-dd-HH}.gz";
    
    private static List<String> getKeywords(String file) throws IOException {
        List<String> keywords = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = null;
            
            while ( (line = br.readLine()) != null ) {
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
                buf.append(",");
            }
            buf.append(keyword[0]);
            buf.append(",");
            buf.append(keyword[1]);
        }
        return buf.toString();
    }
    
    public static void main( String[] args )
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
        
        // Set up the filter query
        FilterQuery query = null;

        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            
            if ( line.hasOption("help") ) {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "StreamFilter", options );
                
                System.exit(1);
            }
            
            // Check if we have keyword file
            if ( line.hasOption("keywords") ) {
                
                String file = line.getOptionValue("keywords");
                
                try {
                    List<String> keywordList = getKeywords(file);
                    
                    if ( query == null ) {
                        query = new FilterQuery();
                    }
                    query.track(keywordList.toArray(new String[0]));
                    
                } catch ( IOException ioe ) {
                    System.err.printf("File [%s] Not Found!", file);

                    System.exit(1);
                }
            }
            
            // Check if we have a geojson file
            if ( line.hasOption("bounds") ) {
                
                String file = line.getOptionValue("bounds");
                
                try {
                    List<Double[]> locationList = readGeoJson(file);
                    double[][] locationListDouble = new double[2][2];
                    
                    Double[] fullBox = locationList.get(0);
                    locationListDouble[0] = new double[]{fullBox[0], fullBox[1]};
                    locationListDouble[1] = new double[]{fullBox[2], fullBox[3]};
                    
                    String locStr = toLocationsString(locationListDouble);
                    System.out.println(locStr);
                    
                    if ( query == null ) {
                        query = new FilterQuery();
                    }
                    query.locations(locationListDouble);
                    
                } catch ( IOException ioe ) {
                    System.err.printf("Error reading GeoJSON File: [%s]", file);
                    ioe.printStackTrace();

                    System.exit(1);
                }
            }
            
            // Check if we have a user file
            if ( line.hasOption("users") ) {
                
                String file = line.getOptionValue("users");
                
                try {
                    List<String> usersList = getKeywords(file);
                    long[] usersListLong = new long[usersList.size()];
                    
                    for ( int i=0; i<usersList.size(); i++ ) {
                        usersListLong[i] = Long.parseLong(usersList.get(i));
                    }
                    
                    if ( query == null ) {
                        query = new FilterQuery();
                    }
                    query.follow(usersListLong);
                } catch ( IOException ioe ) {
                    System.err.printf("File [%s] Not Found!", file);

                    System.exit(1);
                }
            }
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
        
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

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        
        // Set the status listener
        twitterStream.addListener(new StatusListener() {

            @Override
            public void onStatus(Status status) {
//                logger.info(status.toString());
                
                String statusJson = TwitterObjectFactory.getRawJSON(status);
                statusLogger.info(statusJson);
                
                cnt++;
                if (cnt % 1000 == 0) {
                    System.out.println(cnt + " messages received.");
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
//                logger.warn("Hit Delete notice");
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                logger.warn("Hit track limitation");
            }

            @Override
            public void onScrubGeo(long l, long l1) {
                logger.warn("Hit scrub geo");
            }

            @Override
            public void onStallWarning(StallWarning sw) {
                logger.warn("Hit on stall");
            }

            @Override
            public void onException(Exception excptn) {
                System.err.println("Hit Exception: " + excptn.toString());
            }
        });
        
        RawStreamListener rawListener = new RawStreamListener() {

            @Override
            public void onMessage(String rawString) {

            }

            @Override
            public void onException(Exception ex) {
                logger.warn(ex);
            }

        };

        twitterStream.addListener(rawListener);
        
        // If we have a query, filter on it. Otherwise, just stream
        if ( query != null ) {
            System.out.println("Query:" + query);
        
            // Now start listening
            twitterStream.filter(query);
            
        } else {
            twitterStream.sample();
        }
    }
}
