---
title: '<code>ndbc-api</code>: Accelerating oceanography and climate science research with Python'
tags:
  - Python
  - oceanography
  - climate science
  - NDBC
authors:
  - name: Christopher David Jellen
    orcid: 0000-0003-0469-353X
    affiliation: 1
affiliations:
 - name: United States Naval Academy, USA
   index: 1
date: 02 September 2024
bibliography: paper.bib
---

# Summary

The National Data Buoy Center (NDBC) and its partners are an essential source of marine meteorological and oceanographic data `[@NDBC_Data_Guide]`. The `ndbc-api` Python package is an open-source tool designed to streamline the acquisition, synthesis, and analysis of this data. It provides a programmatic interface for accessing real-time and historical observations from a network of buoys, coastal stations, and deployments. This package simplifies the process of retrieving, parsing, and organizing NDBC data, particularly when dealing with multiple stations or extended time ranges, which can be cumbersome using the traditional file-based access methods provided by the NDBC. By offering a user-friendly Python API, this package empowers researchers and practitioners in oceanography, meteorology, and related fields to efficiently integrate NDBC data into their workflows, accelerating research in climate science and oceanography.

# Statement of need

The National Oceanic and Atmospheric Association's National Data Buoy Center maintains marine monitoring and observation stations around the world `[@NDBC_Active_Stations]`. These stations report atmospheric, oceanographic, and other meterological data at regular intervals to the NDBC `[@NDBC_Data_Guide;@NDBC_Active_Stations]`. Measurements are made available over HTTP through the NDBC's data service. Measurements are typically distributed as quality-controlled `utf-8` encoded, station-by-station, fixed-period text files `[@NDBC_Data_Guide]`. While the data collected and maintained by the NDBC is critical to oceanography and climate science researchers, the mode of access adds cost and complexity to their workflows. These challenges are particularly pronounced when working with long-duration data, data from multiple stations, or data with a high proportion of missing measurements.

The `ndbc-api` addresses these critical gap by providing a streamlined, programmatic interface to the NDBC's data service. By abstracting the complexities of file-based access, handling of missing measurements, and cross-station joins, the `ndbc-api` package lowers the barriers to obtaining and using the NDBC's global oceanographic and meterological data in scientific research. Researchers specify their stations, data modalities, and time ranges of interest, and the package returns the processed NDBC data either as a Pandas `DataFrame` object or as a NetCDF4 `Dataset` object `[@pandas; @unidata_netcdf4]`. The package maps missing measurements from their varied text-based identifiers such as `99`, `999`, or `MM`, into a single missing measurement representation of `nan`. The challenge of aligning and joining data across modalities and stations is similarly handled before the final object is returned to the user. By exposing station metadata and search functionality alongside data retrieval methods, researchers are also able to identify the set of stations, based on their NDBC identifier, that were active during a given period, or within some radius of a given location. The combination of efficient identification of the relevant stations, retrieval of the desired modalities, and processing of the data from the NDBC data service make the `ndbc-api` a valuable tool for oceanography and climate science researchers.

# Acknowledgements

The authors would like to acknowledge the invaluable contributions of the National Data Buoy Center (NDBC), their teams, their network of partners. Their efforts in deploying and maintaining the buoys and marine observation stations, collecting and quality-controlling oceanographic data, and making their measurements readily available through their data service are fundamental to advancing our understanding of earth systems. The `ndbc-api` Python package would not be possible without their commitment to open data access and their unwavering support for the scientific and operational communities. We extend our sincere appreciation to everyone involved in this vital endeavor.

# References