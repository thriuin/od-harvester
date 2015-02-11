# od-harvester

Custom data harvester for geospatial sources for Open Canada

Currently, the harvester supports data from

 - Environment Canada's internal CSW server 
 - Natural Resources Canada's Geogratis server (http://geogratis.gc.ca)
 
Database Requirements

The harvester saves working data to a PostgreSQL database. Currently, this database must
be created manually before using the scripts in this project.

The Geogratis scanner will need read/write access to these three tables.

### Getting Started ###

The harvester scripts for Open Data are a collection of Python scripts, and makes use of a small number of 
additional Python libraries. Assuming you are using virtualenv and pip (or equivalent), the required libraries 
are enumerated in the requirements.txt file. To install the required libraries using pip:

```pip install -r requirements.txt```

For more information on how to use virtualenv and pip see:

* [Virtualenv](http://virtualenv.readthedocs.org/en/latest/)
* [Pip](http://pip.readthedocs.org/en/latest/user_guide.html)


### harvester.ini file

The harvester scripts need a number of runtime parameters such as database connection information. Set the
following values in this .ini.

```
[sqlalchemy]
 # This is the SQLAlchemy database connection string to the PostgreSQL database
sqlalchemy.url = postgresql://dbuser:password@hostname/database

[csw]
csw.url = http://geoserv.dept.gc.ca/geonetwork/csw?service=CSW
csw.username = joeuser
csw.password = 1234pass
```

## Scanning Data sources ##

Scanning Geogratis, or other data sources, is a 3 step process

1. Harvest the data from the source and save it into a records table (geogratis_records or ec_records)
   Example: <pre>python gr_scanner.py -m -l scan.log</pre> 
2. Convert the harvested data into the internal format used by CKAN. 
   The CKAN dataset json is generated and saved to the package_updates table.
   Example: <pre>python converter.py -m -t</pre>
3. Dump the CKAN metadata to file in the JSON Lines format. 
   Example: <pre>python dump_packages.py -m -t ec -f mydata.jsonl</pre>
4. Use the ckanapi utility to load the JSON Lines files into the portal
 
### Dataset Metadata ###

This table indicates how CKAN dataset metadata fields are mapped to Geogratris metadata fields

 CKAN                             | Geogratis                            
 -------------------------------- | ------------------------------------ 
 url                              | N/A (Calculated field)               
 url_fra                          | N/A (Calculated field)               
 title                            | title (EN - English record)          
 title_fra                        | title (FR - French record)           
 notes                            | summary (EN)                         
 notes_fra                        | summary (FR)                         
 date_modified                    | updatedDate                          
 data_series_name                 | citation.series (EN)                 
 data_series_name_fra             | citation.series (FR)                 
 keywords (list)                  | keywords (EN)                        
 keywords_fra (list)              | keywords (FR)                        
 spatial                          | geometry (calculated)                
 presentation_form                | citation.presentationForm            
 digital_object_identifier        | citation.otherCitationDetails        
 geographic_region                | categories.urn:iso:place(calculated) 
 data_series_issue_identification | citation.seriesIndex                 
 presentation_form                | citation.prsentationForm             
 browse_graphic_url               | browseImages                 
 topic_category (list)            | topicCategories (list)               
 state                            | deleted      

### Resource Metadata (list) ###

This table indicates how CKAN resource metadata fields are mapped to Geogratris metadata fields

 CKAN                             | Geogratis                           
 -------------------------------- | ------------------------------------ 
 name                             | files[].description (EN)            
 name_fra                         | files[].description (FR)            
 url                              | files[].link                        
 format                           | files[].type                        


