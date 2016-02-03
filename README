# Compress and Store the Twitter Public Sample

## Intro
This project allows you to run a set-it-and-forget-it system for streaming the Twitter public sample stream to a local set of gzipped files.
Each file will contain the JSON for all the tweets from the 1% stream.

## Building
This project was written with Maven, so you should be able to do `mvn package` and use the resulting jar file `TwitterStreamFilter-1.0-SNAPSHOT-jar-with-dependencies.jar` in the `target` directory.

## Running
You have a few options with this code. If you run it without arguments, it should download the unfiltered 1% stream. 
You can run code for that as follows:
- `java -Xmx1536m -jar TwitterStreamFilter-1.0-SNAPSHOT-jar-with-dependencies.jar`

You can also provide a file with keywords to track (separated by a newline) using the --keywords/-k flag, a GeoJSON file to get tweets within a geographic area using the --bounds/-b flag, or the path to a file with a list of user IDs to track with the --users/-u flag.

While running, this code will produce several files: `warnings.log.YYYY-MM-DD-HH`, `statuses.log.YYYY-MM-DD-HH`, and many *.gz files. At the end of every hour, the current warnings.log.* and statuses.log.* will be gzipped automatically.