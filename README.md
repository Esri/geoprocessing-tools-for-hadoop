geoprocessing-tools-for-hadoop
===============

The __Geoprocessing Tools for Hadoop__ provides tools to help integrate ArcGIS with Hadoop. More specifically, 
tools are provided that:
* Enable the exchange of data between an 
[ArcGIS Geodatabase]
(http://resources.arcgis.com/en/help/main/10.2/index.html#/What_is_a_geodatabase/003n00000001000000/) and a 
Hadoop system, and 
* Allow ArcGIS users to run Hadoop workflow jobs.

See these tools in action as part of the [samples](https://github.com/Esri/gis-tools-for-hadoop/tree/master/samples) in [GIS Tools for Hadoop](https://github.com/Esri/gis-tools-for-hadoop).

## Features

* Tools to convert between Feature Classes in a Geodatabase and JSON formatted files.
* Tools that copy data files from ArcGIS to Hadoop, and copy files from Hadoop to ArcGIS.
* Tools to run an [Oozie](http://oozie.apache.org/) workflow in Hadoop, and to check the status of a submitted 
workflow.

## Wiki and Tutorials

* [Wiki](https://github.com/Esri/geoprocessing-tools-for-hadoop/wiki) of available geoprocessing tools.
* [Tutorials](https://github.com/Esri/gis-tools-for-hadoop/wiki) on how to run the geoprocessing tools.

## Instructions
1. Download this repository as a .zip file and unzip to a suitable location or clone the repository with a git tool.
2. WebHDFS and Requests libraries in the tool folder are provided for convenience. If you know you will using libraries installed in /site-packages folder then remove 'webhdfs' or 'requests' folders - otherwise do not.
3. In the ‘ArcToolbox’ pane of [ArcGIS Desktop](http://www.esri.com/software/arcgis/arcgis-for-desktop/), 
use the [‘Add Toolbox…’ command](http://resources.arcgis.com/en/help/main/10.2/index.html#//003q0000001m000000) 
to add the Hadoop Tools toolbox (the HadoopTools.pyt file you saved in step 1) file 
into ArcGIS Desktop.
4. Use the tools individually, or use them in models and scripts, such as the examples 
in: [GIS Tools for Hadoop](https://github.com/Esri/gis-tools-for-hadoop).

## Requirements

* ArcGIS 10.1 or later.
* A Hadoop system with WebHDFS support.

## Dependencies
* WebHDFS and Requests are bundled-in and reside in the tool folder.
* A Python library webhdfs-py is required for WebHDFS support.  Source is located 
at [webhdfs-py](https://github.com/Esri/webhdfs-py).
* The Requests python library is required for OozieUtils.py (installation doc is located 
at http://docs.python-requests.org/en/latest/user/install/#install).

## Resources

* [GeoData Blog on the ArcGIS Blogs](http://blogs.esri.com/esri/arcgis/author/jonmurphy/)
* [Big Data Place on GeoNet](https://geonet.esri.com/groups/big-data)
* [ArcGIS Geodata Resource Center]( http://resources.arcgis.com/en/communities/geodata/)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing)

## Licensing
Copyright 2013-2019 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's 
[license.txt](https://raw.github.com/Esri/hadoop-gp-tools/master/license.txt) file.
