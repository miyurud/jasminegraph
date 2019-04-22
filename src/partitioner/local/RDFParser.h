/**
Copyright 2019 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */


#ifndef JASMINEGRAPH_RDFPARSER_H
#define JASMINEGRAPH_RDFPARSER_H


#include <xercesc/util/PlatformUtils.hpp>
#include "../../util/Utils.h"


#include <xercesc/dom/DOM.hpp>
#include <xercesc/dom/DOMDocument.hpp>
#include <xercesc/dom/DOMDocumentType.hpp>
#include <xercesc/dom/DOMElement.hpp>
#include <xercesc/dom/DOMImplementation.hpp>
#include <xercesc/dom/DOMImplementationLS.hpp>
#include <xercesc/dom/DOMNodeIterator.hpp>
#include <xercesc/dom/DOMNodeList.hpp>
#include <xercesc/dom/DOMNode.hpp>
#include <xercesc/dom/DOMText.hpp>

#include <xercesc/parsers/XercesDOMParser.hpp>
#include <xercesc/util/XMLUni.hpp>

#include <string>
#include <stdexcept>
#include <map>
#include <list>
#include <vector>
#include <fstream>
#include <iostream>

using std::string;
using namespace std;

// Error codes

enum {
    ERROR_ARGS = 1,
    ERROR_XERCES_INIT,
    ERROR_PARSE,
    ERROR_EMPTY_DOCUMENT
};

class GetConfig {
public:

    GetConfig();

    ~GetConfig();

    void readConfigFile(std::string &, int id) throw(std::runtime_error);


    char *getOptionA() { return m_OptionA; };

    char *getOptionB() { return m_OptionB; };

    void writeEdgesToFile();

    long addToAuthors(std::map<std::string, long> *map, std::string URI);

    long addToEdges(std::map<pair<int, int>, int> *map, long node_1, long node_2, long article_id);

    long addToArticles(std::map<std::string, long> *map, std::string URI);

    static std::map<std::pair<int, int>, int> getEdgeMap();

    static std::map<long, string[7]> getAttributesMap();


private:
    Utils utils;
    int graphID;
    xercesc::XercesDOMParser *m_ConfigFileParser;
    char *m_OptionA;
    char *m_OptionB;
    char *Author_name;
    char *paper_title;


    // Internal class use only. Hold Xerces data in UTF-16 SMLCh type.

    XMLCh *TAG_root;

    XMLCh *TAG_ApplicationSettings;
    XMLCh *ATTR_OptionA;
    XMLCh *ATTR_OptionB;

    XMLCh *TAG_has_author;
    XMLCh *TAG_has_title;
    XMLCh *TAG_article_of_journal;
    XMLCh *TAG_Journal;
    XMLCh *TAG_has_volume;
    XMLCh *TAG_has_web_address;
    XMLCh *TAG_has_date;
    XMLCh *TAG_Calendar_date;
    XMLCh *TAG_year_of;
    XMLCh *TAG_month_of;


    XMLCh *TAG_RDF;
    XMLCh *TAG_Article_Reference;
    XMLCh *TAG_Book_Section_Reference;
    XMLCh *TAG_Thesis_Reference;
    XMLCh *TAG_Book_Reference;
    XMLCh *TAG_Conference_Proceedings_Reference;

    XMLCh *TAG_full_name;
    XMLCh *TAG_Person;

    std::map<string, long> authors;
    vector<pair<int, int> > edgelist;

    std::map<long, string> authorsTemp;
    std::map<string, long> articles;
    std::map<long, string> articlesTemp;
    //std::map<long, string[7]> articlesMap;
    string attributes[7];

};


#endif //JASMINEGRAPH_RDFPARSER_H
