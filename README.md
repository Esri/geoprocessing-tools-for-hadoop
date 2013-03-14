gp-tools-hadoop
===============

The GP Tools for Hadoop provides tools to exchange features between a Geodatabase and Hadoop and run Hadoop workflow jobs.

## Features
* Tools that export features from ArcGIS to Hadoop, and import features from Hadoop to ArcGIS
* Tools to submit a workflow to process data in Hadoop, and to check the status of a workflow
* Tools to convert features to JSON (Will eventually use JSON Conversion tools in the ArcGIS Geoprocessing Data Management Toolbox)

## Instructions
1. Download this repository as a .zip file and unzip to a suitable location or clone the repository with a git tool.
2. Verify the Python dependencies are met, installing WebHDFS and Requests libraries if needed.
3. In the ‘ArcToolbox’ pane of ArcGIS desktop, use the ‘Add Toolbox…’ command to add the Hadoop Tools toolbox (the HadoopTools.pyt file you saved in step 1) file into ArcGIS Desktop.
4. Use the tools individually, or use them in models and scripts, such as the examples in: [Hadoop-Spatial-Tools](https://github.com/Esri/hadoop-spatial-tools)

## Requirements

* ArcGIS 10.1 or later.
* A Hadoop system with WebHDFS support.

## Dependencies
* For WebHDFS support, a Python library webhdfs-py is bundled in.  Source is located at [webhdfs-py](https://github.com/Esri/webhdfs-py)
* Requests python library is required for OozieUtils.py (installation doc is located at http://docs.python-requests.org/en/latest/user/install/#install)

## Resources

* [ArcGIS Geodata Resource Center]( http://resources.arcgis.com/en/communities/geodata/)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Anyone and everyone is welcome to contribute. 

## Licensing
Copyright 2013 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt]( https://raw.github.com/Esri/hadoop-gp-tools/master/license.txt) file.

[](Esri Tags: ArcGIS, Geoprocessing, GP, Hadoop, Spatial, Python)
[](Esri Language: Python)
