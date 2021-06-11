#!/usr/bin/python3
import sys, logging
from yaml import load as load_yaml
from json import loads as json_loads, dumps as json_dumps
from re import match, escape
from os import environ
from requests import Session, Request

''' Log(config)
Allows for configurable logging with extra logic applied
Methods can be expanded for additional logging requirements
'''
class Log(object):

    def __init__(self, config):

        log_file     = config.get('file')
        enable_debug = config.get('debug')

        # Instantiate a Logger and StreamHandler for it
        logger = logging.getLogger('tsds-telegraf')
        sh     = logging.StreamHandler()

        # Set the logfile
        if log_file:
            logging.basicConfig(filename=log_file)

        # Set the logging level
        if enable_debug:
            logger.setLevel(logging.DEBUG)
            sh.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
            sh.setLevel(logging.INFO)

        # Define the log output format and then add the StreamHandler to the Logger
        sh.setFormatter(logging.Formatter('[%(name)s] [%(levelname)s]: %(message)s'))
        logger.addHandler(sh)

        self.logger     = logger
        self.debug_mode = enable_debug

    # Logger Get & Set
    @property
    def logger(self):
        return self.__logger
    @logger.setter
    def logger(self, logger):
        self.__logger = logger

    # Debug Mode Get & Set
    @property
    def debug_mode(self):
        return self.__debug_mode
    @debug_mode.setter
    def debug_mode(self, debug_mode):
        self.__debug_mode = debug_mode

    # Helper method to pretty print data structures
    def _dumper(self, data):
        try:
            return json_dumps(data)
        except TypeError as e:
            self.logger.error('Could not create data dump for logging: {}'.format(e))
            return data

    # Define the logging methods of the configured logger
    # The check is an optimization to reduce message evaluation
    # Error logging is always available and has no check
    def debug(self, msg, dump=False):
        if self.logger.isEnabledFor(logging.DEBUG):
            msg = self._dumper(msg) if dump else msg
            self.logger.debug(msg)

    def info(self, msg, dump=False):
        if self.logger.isEnabledFor(logging.INFO):
            msg = self._dumper(msg) if dump else msg
            self.logger.info(msg)

    def warn(self, msg, dump=False):
        if self.logger.isEnabledFor(logging.WARNING):
            msg = self._dumper(msg) if dump else msg
            self.logger.warning(msg)

    def error(self, msg, dump=False):
        msg = self._dumper(msg) if dump else msg
        self.logger.error(msg)


''' Client(config, Log)
Allows for easy creation of a configurable web service client
Currently hard-coded to only support TSDS push services.
'''
class Client(object):

    def __init__(self, config, log):
        
        # get client information from client segment in config
        self.username = config.get('username')
        self.password = config.get('password')
        self.url      = config.get('url')
        self.timeout  = config.get('timeout')

        # get environment variable names from same client segment
        username_env_var = config.get('username_env_var')
        password_env_var = config.get('password_env_var')
        url_env_var      = config.get('url_env_var')

        # override client info if environment vars are being used
        if username_env_var in environ:
            self.username = environ[username_env_var]

        if password_env_var in environ:
            self.password = environ[password_env_var]

        if url_env_var in environ:
            self.url = environ[url_env_var]


        # Create a Session and Request object used for POSTing requests
        self.session      = Session()
        self.session.auth = (self.username, self.password)

        self.log = log
        self.log.debug('Initialized Client instance')

    # Username Get & Set
    @property
    def username(self):
        return self.__username
    @username.setter
    def username(self, username):
        self.__username = username

    # Password Get & Set
    @property
    def password(self):
        return self.__password
    @password.setter
    def password(self, password):
        self.__password = password

    # URL Get & Set
    @property
    def url(self):
        return self.__url
    @url.setter
    def url(self, url):
        url = url if url[-1] != '/' else url[:-1]
        self.__url = url

    # Timeout Get & Set
    @property
    def timeout(self):
        return self.__timeout
    @timeout.setter
    def timeout(self, timeout):
        self.__timeout = int(timeout) if timeout else 15

    # Session Get & Set
    @property
    def session(self):
        return self.__session
    @session.setter
    def session(self, session):
        self.__session = session

    # Request Get & Set
    @property
    def request(self):
        return self.__request
    @request.setter
    def request(self, request):
        self.__request = request

    # Log Get & Set
    @property
    def log(self):
        return self.__log
    @log.setter
    def log(self, log):
        self.__log = log

    # Takes data and pushes its JSON string to TSDS via POST
    # Return will evaluate to true if an error occurred
    def push(self, data):
      
        # Stringify the data for POSTing
        try:
            data_str = json_dumps(data)
        except RuntimeError as e:
            self.log.error('Error while attempting to create JSON string from data: {}\n{}'.format(data, e))
            return 1

        # Create the data dict for requests to POST
        post_data = {'method': 'add_data', 'data': data_str}

        # POST the data to the TSDS push service URL
        try:

            # Update the PreparedRequest's data payload
            req = Request('POST', self.url, data=post_data)
            req = self.session.prepare_request(req)

            # Send the prepared POST request
            res = self.session.send(req, timeout=self.timeout)

            # Raise an error when a 4XX or 5XX status code was received
            res.raise_for_status()

        except RuntimeError as e:
            self.request.data = None
            self.log.error('Error while attempting to POST data: {}'.format(e))
            return 1
    
        self.log.info('Pushed {} updates to TSDS'.format(len(data)))
        if self.log.debug_mode and len(data) > 0:
            self.log.debug('Sample update from batch:')
            self.log.debug(data[0])

        return
        

''' DataTransformer(collections, Log)
Uses configurable definitions to translate Telegraf metrics to TSDS measurements.
Performs data transormations including rate calculations.
'''
class DataTransformer(object):

    # More than meets the eye
    def __init__(self, collections, log):
        self.collections = collections
        self.log         = log
        self.cache       = {}

        self.log.debug('Initialized DataTransformer instance')

    # Collections Get & Set
    @property
    def collections(self):
        return self.__collections
    @collections.setter
    def collections(self, collections):
        self.__collections = collections

    # Log Get & Set
    @property
    def log(self):
        return self.__log
    @log.setter
    def log(self, log):
        self.__log = log

    # Cache Get & Set
    @property
    def cache(self):
        return self.__cache
    @cache.setter
    def cache(self, cache):
        self.__cache = cache if isinstance(cache,dict) else dict()

    # Transforms a JSON string from Telegraf into a list of data dicts for TSDS ingestion
    def transform(self, json_str):

        # Returned output will be a list of data dicts
        output = []

        # Attempt to load the JSON string as a dict
        try:
            data = json_loads(json_str)
        except RuntimeError as e:
            self.log.error('Unable to parse JSON string from STDIN, skipping ({}): {}'.format(line, e))
            return output

        # Get the Telegraf data components
        name      = data.get('name')
        fields    = data.get('fields')
        tags      = data.get('tags')
        timestamp = data.get('timestamp')
        
        # Verify data components are present
        verify_err  = 'Received data with missing component(s): {}'
        verify_miss = []
        if name == None:
            verify_miss.append('"name"')
        if fields == None:
            verify_miss.append('"fields"')
        if tags == None:
            verify_miss.append('"tags"')          
        if timestamp == None:
            verify_miss.append('"timestamp"')
        # Log an error and return the empty output array 
        if len(verify_miss) > 0:
            self.log.error(verify_err.format(', '.join(verify_miss)))
            return output

        # Get the collection configuration by using its name from the data
        collection = self.collections.get(name, False)

        # Check whether the collection type has configurations
        if not collection:
            self.log.error('Collection "{}" is not configured!'.format(name))
            return output

        # Initialize a cache for the collection type when it doesn't exist
        if name not in self.cache:

            # The collection's cache has an ordering of all defined metadata keys
            # Telegraf metrics can contain disjointed data due to async replies or packet sizing
            self.cache[name] = {}

        # Get a metadata dictionary
        metadata, meta_errors = self._parse_metadata(collection, tags)
        if meta_errors:
            return output

        # Get or create a dict for the metadata combination within the collection type's cache
        # This dict is used for value processing
        meta_key    = name + '|' + "|".join(sorted(metadata.values()))
        cache_entry = self.cache[name].setdefault(meta_key, {})

        interval = collection.get('interval')

        # Use the Telegraf field maps to build value data for TSDS
        values = {}
        for field_map in collection.get('fields', []):
            
            field_name = field_map['from']
            value_name = field_map['to']

            # Pull the value for the Telegraf field
            value = fields.get(field_name, None)

            # TODO: How should missing field_names be handled?
            # Verify that we have a value for the requested field_name
            if value == None:
                #self.log.debug('No value for requested field name "{}"'.format(field_name))
                values[value_name] = None
                continue

            # Apply rate calculations
            if 'rate' in field_map:
                value = self._calculate_rate(\
                    cache_entry,\
                    value_name,\
                    int(timestamp),\
                    value,\
                    interval,\
                    meta_key\
                )

            # Set the value data for the TSDS value name
            values[value_name] = value

        # Create a dict of data to push to TSDS and add it to the output
        tsds_data = {
            "meta":     metadata,
            "time":     int(timestamp),
            "values":   values,
            "interval": interval,
            "type":     collection.get('tsds_name')
        }
        output.append(tsds_data)

        # Return here unless we want optional metadata
        if 'optional_metadata' not in collection:
            return output

        # Flag to indicate optional metadata fields are present
        has_opt = False

        # Check for any optional metadata in the Telegraf tags
        # TODO: This left in for backward compatability
        #       but should be adjusted to use the "optional" flag
        #       for metadata instead
        for opt_meta in collection.get('optional_metadata', []):

            tag_name  = opt_meta['from']
            meta_name = opt_meta['to']

            # Absolute match for tag names
            if tag_name in tags:
                metadata[meta_name] = tags[tag_name]

            # Wildcard matching for tag names
            elif "*" in tag_name:

                # Get the data for each Telegraf tag that matches our wildcard
                optional_metadata = [tags[t] for t in tags if re.match(tag_name, t)]

                # Map matches to a specified field_name if configured
                if opt_meta.get('field_name'):
                    optional_metadata = [{opt_meta['field_name']: m} for m in optional_metadata]

                if len(optional_metadata):
                    metadata[meta_name] = optional_metadata
                    has_opt = True

        # Add a separate object for optional metadata to the output for TSDS
        if has_opt:
            metadata_data = {
                "meta": metadata,
                "time": timestamp,
                "type": collection.get('tsds_name') + ".metadata"
            }
            output.append(metadata_data)

        self.log.debug('Transform produced the following data:')
        self.log.debug(output, True)
            
        return output


    def _parse_metadata(self, collection, tags):
        '''
        Create a dict of the metadata TSDS names mapped to the values from Telegraf.
        Telegraf tags are a map of metadata fieldnames to their values.
        '''
        metadata = {}
        errors   = 0
        for c in collection.get('metadata'):
             
            tag_name  = c.get('from')
            meta_name = c.get('to')
            optional  = c.get('optional')
            value     = tags.get(tag_name)

            if value != None:
                metadata[meta_name] = value

            elif not optional:
                errors += 1

        if errors:
            self.log.debug('{} data missing {} metadata values'.format(collection.get('tsds_name'),errors))

        return metadata, errors


    def _calculate_rate(self, cache_entry, value_name, timestamp, value, interval, meta_key):
        '''
        Calculate a rate value using the current value, last cached value, and interval.
        '''

        # Retrieve data from the cache
        (last_timestamp, last_value) = cache_entry.setdefault(value_name, (timestamp, None))
        cache_entry[value_name] = (timestamp, value)

        # No current value or no previous value we return after updating the cache
        if value == None or last_value == None:
            return None

        # Ensure the value is a float for calculations
        value = float(value)

        # Calculate the time delta
        delta = int(timestamp - last_timestamp)

        # Handle errors where the delta value is negative or zero
        if delta <= 0:
            return None

        # Check whether the delta falls within an expected time interval
        if delta > interval:
            msg = 'ERROR: Rate delta exceeds expected interval for key "{}" [INTERVAL {} | DELTA {} | CURRENT {} | LAST {}]'
            self.log.error(msg.format(meta_key, interval, delta, timestamp, last_timestamp))

        # Calculate the change in the value
        delta_value = value - last_value

        # Handle counter overflow/reset here
        if value < last_value:

            # 64-bit counters
            if value > 2**32:
                delta_value = 2**64 - last_value + value
            # 32-bit counters
            else:
                delta_value = 2**32 - last_value + value
        
        # Calculate the rate
        rate = delta_value / delta
        
        return rate
    

''' Main processing loop.
Takes config file from command-line arguments to configure classes.
Reads Telegraf JSON input from STDIN and produces TSDS updates in batches.
'''
if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print("Usage: {} <config file>".format(sys.argv[0]))
        sys.exit(1)
    else:
        config_file = sys.argv[1]

        # Read the YAML configuration
        with open(config_file) as f:
            config = load_yaml(f)

    # Instantiate the Config, Log, Client, and DataTransform objects
    L    = Log(config.get('logging'))
    TSDS = Client(config.get('client'), L)
    DT   = DataTransformer(config.get('collections'), L)

    L.info('Initialized TSDS-Telegraf execd plugin')

    # Batch array for incoming JSON storage
    batch = []
    
    # Get the number of updates to send in a batch
    batch_size = config.get('batch_size', 10)

    # Process each line from STDIN
    for line in sys.stdin:

        # Applies any data transformations and rate calculations
        # Provides an array of data dicts to add to the batch
        updates = DT.transform(line)

        if len(updates) == 0:
            L.warn('Line from STDIN did not produce any update messages: {}'.format(line))
            continue

        # Adds the resulting TSDS update dicts to the batch
        batch.extend(updates)

        # Push the updates batch once it has reached an appropriate size
        if len(batch) >= batch_size:

            err = TSDS.push(batch)
            batch = []

            if err:
                sys.exit()
