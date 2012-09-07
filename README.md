# lein-emr

A Leiningen plugin used to generate Amazon Elastic MapReduce jobflows. It is a wrapper over Amazon's 
[elastic-mapreduce](http://aws.amazon.com/code/Elastic-MapReduce/2264)
client. It can get very tedious to write out all the options to their script; this plugin tries to make launching a 
jobflow extremely easy.

## Usage

Put `[lein-emr "0.1.0-SNAPSHOT"]` into the `:plugins` vector of your project.clj.

Example use:

    $ lein emr -n "namehere" -t "large" -s 10 -b 0.2 -bs bsconfig.xml

All options (you can see this with 'lein emr help'):
    
    Switches:        Defaults:    Description:                                 
    -n, --name       dev          Name of cluster.                      
    -t, --type       high-memory  Type  cluster.                        
    -s, --size                    Size of cluster.                      
    -z, --zone       us-east-1d   Specifies an availability zone.       
    -m, --mappers                 Specifies number of mappers.          
    -r, --reducers                Specifies number of reducers.         
    -b, --bid                     Specifies a bid price.                
    -d, --on-demand               Uses on demand-pricing for all nodes.
    -bs, --bootstrap              Bootstrap config file location.  
 
All of your bootstrap actions should be in an xml config file. Just pass the location of that file to lein emr via -bs 
or --bootstrap. Heres an example config file:

    <bootstrapactions>
    <action script="s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive">
    </action>
    <action script="s3://elasticmapreduce/bootstrap-actions/add-swap">
        <arg>2048</arg>
    </action>
    <action script="s3://elasticmapreduce/bootstrap-actions/configure-hadoop"
         site-config-file="s3://myawsbucket/bootstrap-actions/config.xml">
    </action>
    <action script="s3://myawsbucket/bootstrap-actions/custom_bootstrap.sh">
    </action>
    </bootstrapactions>

You can read more about Amazon Bootstrap Actions on their 
[Developer Guide](http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/Bootstrap.html). You can 
specify up to 16 bootstrap actions (with arguments for each) in your config file. The 'configure-hadoop' bootstrap 
action has a special argument, the site config file, which should be specified as an attribute rather than an arg 
in the xml.

## Sensible Defaults

If you do not specify 'on-demand' or provide a bid price, then it will default to sensible bid prices so that you may 
still get the current spot price on your core nodes.

Default Bid Prices
    * Large: 0.32
    * Cluster-Compute: 1.30
    * High-Memory: 1.80

Mappers / Reducers
    * Large: (4 max map tasks, 2 max reduce taks)
    * High-Memory: (30 max map tasks, 24 max reduce tasks)
    * Cluster-Compute: (22 max map tasks, 16 max reduce tasks)

## Install Amazon's Elastic MapReduce Client
Go to the [Elastic MapReduce client page](http://aws.amazon.com/code/Elastic-MapReduce/2264) and download the script. 
You should take a look at their 
[Getting Started Guide](http://docs.amazonwebservices.com/ElasticMapReduce/latest/GettingStartedGuide/SignUp.html)-- but 
here is a quick summary:

Create a file named credentials.json in the elastic-mapreduce-cli/elastic-mapreduce-ruby directory (that you just 
downloaded). The credentials file should contain all of your AWS credentials (secret key, etc).

Add elastic-mapreduce to your system path:

    export PATH=$PATH:<directory_where_you_unzipped_elastic_mapreduce_client>

Make sure that you are using Ruby 1.8, their client DOES NOT work with ruby 1.9! To find out which version of ruby you 
have:

    which ruby
    
or
    ruby -version
    

If you dont have it already, download [RVM, the Ruby Version Manager](https://rvm.io) to help with mutliple versions of
Ruby. If you need to switch your Ruby version, you can do:

    rvm install 1.8.7
    rvm use 1.8.7

Unfortunately if you have Mac OS X 10.8+ or Xcode 4.2+, you might run into an error since these systems do not ship w
ith gcc (which ruby requires), but use llvm-gcc instead. If you have this issue then you can install apple-gcc42 and 
required libraries using Homebrew:

    brew update
    brew tap homebrew/dupes
    brew install autoconf automake apple-gcc42
    rvm pkg install openssl

If you have trouble linking gcc, you might need to change permissions for usr/local/lib to read/write. Check to make 
sure you have gcc by:

    which gcc-4.2

Now run 

    rvm install 1.8.7

If you get a 'make error', see 
[this thread](http://stackoverflow.com/questions/11664835/mountain-lion-rvm-install-1-8-7-x11-error).

## License

Copyright © 2012 David Petrovics

Distributed under the Eclipse Public License, the same as Clojure.
