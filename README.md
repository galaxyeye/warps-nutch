Qiwur Nutch is based on Apache Nutch 2.3.0, with awesome features : 


1. Crowdsourcing crawl support

2. Ajax support

3. Humanoid bot

4. Better system counters

5. Better web UI


This project works together with the other two relative projects : 
satellite : https://github.com/galaxyeye/satellite

qiwur-nutch-ui : https://github.com/galaxyeye/qiwur-nutch-ui

Project satellite is a fetcher client based on phantomjs, which visits
the target web site just like real human being.

Project qiwur-nutch-ui is a PHP based WEB UI for nutch.


To run crawler using crowdsourcing mode : 

1. make sure you are familiar with Apache Nutch

2. modify nutch-site.xml, set "fetcher.fetch.mode" to be "crowdsourcing", 
   set "nutch.master.domain" to be the machine you run nutch server

3. start satellite on any machine follow satellite's README. Satellite is desinged to
   run on any cheap PC, it acts as a "normal browser" just like you visit a website
   using chrome

4. you can run crawl locally following the Apache Nutch's official guide, using command line.
   when the fetch job runs, it also starts nutch server if necessary. Once nutch server
   is running, the satellite will ask nutch server for tasks.

   you can also run nutch server using ./bin/nutch nutchserver, if so, you need 
   nutch ui project to start a crawl, note : currently, nutch ui project is not only
   used just for nutch, but also used for other propurse, just ignore the 
   functionalties has no relativity with nutch.



=========================================================================================
Apache Nutch README

For the latest information about Nutch, please visit our website at:

   http://nutch.apache.org

and our wiki, at:

   http://wiki.apache.org/nutch/

To get started using Nutch read Tutorial:

   http://wiki.apache.org/nutch/Nutch2Tutorial
   
Export Control

This distribution includes cryptographic software.  The country in which you 
currently reside may have restrictions on the import, possession, use, and/or 
re-export to another country, of encryption software.  BEFORE using any encryption 
software, please check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to see if this is 
permitted.  See <http://www.wassenaar.org/> for more information. 

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has 
classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which 
includes information security software using or performing cryptographic functions with 
asymmetric algorithms.  The form and manner of this Apache Software Foundation 
distribution makes it eligible for export under the License Exception ENC Technology 
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, 
Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software:

Apache Nutch uses the PDFBox API in its parse-tika plugin for extracting textual content 
and metadata from encrypted PDF files. See http://pdfbox.apache.org for more 
details on PDFBox.

