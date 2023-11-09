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

#include "RDFParser.h"

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

using namespace xercesc;
using namespace std;

/**
 *  Constructor initializes xerces-C libraries.
 *  The XML tags and attributes which we seek are defined.
 *  The xerces-C DOM parser infrastructure is initialized.
 */
static std::map<pair<int, int>, int> edgeMap;
static std::map<long, string[7]> articlesMap;

GetConfig::GetConfig() {
    try {
        XMLPlatformUtils::Initialize();  // Initialize Xerces infrastructure
    } catch (XMLException &e) {
        char *message = XMLString::transcode(e.getMessage());
        cerr << "XML toolkit initialization error: " << message << endl;
        XMLString::release(&message);
    }

    // Tags and attributes used in XML file.
    // Can't call transcode till after Xerces Initialize()
    TAG_root = XMLString::transcode("root");
    TAG_RDF = XMLString::transcode("rdf:RDF");
    TAG_has_author = XMLString::transcode("akt:has-author");

    TAG_has_title = XMLString::transcode("akt:has-title");
    TAG_article_of_journal = XMLString::transcode("akt:article-of-journal");
    TAG_Journal = XMLString::transcode("akt:Journal");
    TAG_has_volume = XMLString::transcode("akt:has-volume");
    TAG_has_web_address = XMLString::transcode("akt:has-web-address");
    TAG_Calendar_date = XMLString::transcode("akts:Calendar-Date");
    TAG_year_of = XMLString::transcode("akts:year-of");
    TAG_month_of = XMLString::transcode("akts:month-of");
    TAG_has_date = XMLString::transcode("akt:has-date");

    TAG_Article_Reference = XMLString::transcode("akt:Article-Reference");
    TAG_Book_Section_Reference = XMLString::transcode("akt:Book-Section-Reference");
    TAG_Thesis_Reference = XMLString::transcode("akt:Thesis-Reference");
    TAG_Book_Reference = XMLString::transcode("akt:Book-Reference");
    TAG_Conference_Proceedings_Reference = XMLString::transcode("akt:Conference-Proceedings-Reference");

    TAG_Person = XMLString::transcode("akt:Person");
    TAG_full_name = XMLString::transcode("akt:full-name");

    m_ConfigFileParser = new XercesDOMParser;
}

/**
 *  Class destructor frees memory used to hold the XML tag and
 *  attribute definitions. It als terminates use of the xerces-C
 *  framework.
 */

GetConfig::~GetConfig() {
    // Free memory

    delete m_ConfigFileParser;

    try {
        XMLString::release(&TAG_root);

        XMLString::release(&TAG_RDF);
        XMLString::release(&TAG_has_author);
        XMLString::release(&TAG_has_title);
        XMLString::release(&TAG_article_of_journal);
        XMLString::release(&TAG_Journal);
        XMLString::release(&TAG_has_volume);
        XMLString::release(&TAG_has_web_address);
        XMLString::release(&TAG_Calendar_date);
        XMLString::release(&TAG_year_of);
        XMLString::release(&TAG_month_of);
        XMLString::release(&TAG_has_date);
        XMLString::release(&TAG_Article_Reference);
        XMLString::release(&TAG_Book_Section_Reference);
        XMLString::release(&TAG_Thesis_Reference);
        XMLString::release(&TAG_Book_Reference);
        XMLString::release(&TAG_Conference_Proceedings_Reference);
        XMLString::release(&TAG_Person);
        XMLString::release(&TAG_full_name);
    } catch (...) {
        cerr << "Unknown exception encountered while trying to release unknown TAG" << endl;
    }

    // Terminate Xerces

    try {
        XMLPlatformUtils::Terminate();  // Terminate after release of memory
    } catch (xercesc::XMLException &e) {
        char *message = xercesc::XMLString::transcode(e.getMessage());

        cerr << "Unknown exception encountered" << message << endl;
        XMLString::release(&message);
    }
}

/**
 *  This function:
 *  - Tests the access and availability of the XML configuration file.
 *  - Configures the xerces-c DOM parser.
 *  - Reads and extracts the pertinent information from the XML config file.
 *
 *  @param in configFile The text string name of the HLA configuration file.
 */

void GetConfig::readConfigFile(string &configFile, int graphId) {
    /* throw(std::runtime_error) */
    struct stat fileStatus;
    this->graphID = graphId;
    ofstream file;

    errno = 0;
    if (stat(configFile.c_str(), &fileStatus) == -1) {
        if (errno == ENOENT)  // errno declared by include file errno.h
            throw(std::runtime_error("Path file_name does not exist, or path is an empty string."));
        else if (errno == ENOTDIR)
            throw(std::runtime_error("A component of the path is not a directory."));
        else if (errno == ELOOP)
            throw(std::runtime_error("Too many symbolic links encountered while traversing the path."));
        else if (errno == EACCES)
            throw(std::runtime_error("Permission denied."));
        else if (errno == ENAMETOOLONG)
            throw(std::runtime_error("File can not be read\n"));
    }

    // Configure DOM parser.

    m_ConfigFileParser->setValidationScheme(XercesDOMParser::Val_Never);
    m_ConfigFileParser->setDoNamespaces(false);
    m_ConfigFileParser->setDoSchema(false);
    m_ConfigFileParser->setLoadExternalDTD(false);

    try {
        m_ConfigFileParser->parse(configFile.c_str());
        DOMDocument *xmlDoc = m_ConfigFileParser->getDocument();

        DOMElement *elementRoot = xmlDoc->getDocumentElement();
        if (!elementRoot) throw(std::runtime_error("empty XML document"));

        DOMNodeList *children = elementRoot->getChildNodes();
        const XMLSize_t nodeCount = children->getLength();

        for (XMLSize_t xx = 0; xx < nodeCount; ++xx) {
            std::vector<long> authorsInArticle;
            long articleID;
            string web_address;
            string journal_name;
            string year;
            string month;
            string title;
            string type;
            string volume;

            DOMNode *currentNode = children->item(xx);
            if (currentNode->getNodeType() &&  // is not NULL and is element
                currentNode->getNodeType() == DOMNode::ELEMENT_NODE) {
                // Found node which is an Element. Re-cast node as element
                DOMElement *currentElement = dynamic_cast<xercesc::DOMElement *>(currentNode);
                if (XMLString::equals(currentElement->getTagName(), TAG_Article_Reference) ||
                    XMLString::equals(currentElement->getTagName(), TAG_Book_Reference) ||
                    XMLString::equals(currentElement->getTagName(), TAG_Thesis_Reference) ||
                    XMLString::equals(currentElement->getTagName(), TAG_Book_Section_Reference) ||
                    XMLString::equals(currentElement->getTagName(), TAG_Conference_Proceedings_Reference)) {
                    if (XMLString::equals(currentElement->getTagName(), TAG_Article_Reference)) {
                        type = "0";
                    } else if (XMLString::equals(currentElement->getTagName(), TAG_Book_Reference)) {
                        type = "1";

                    } else if (XMLString::equals(currentElement->getTagName(), TAG_Thesis_Reference)) {
                        type = "2";
                    } else if (XMLString::equals(currentElement->getTagName(), TAG_Book_Section_Reference)) {
                        type = "3";

                    } else if (XMLString::equals(currentElement->getTagName(), TAG_Conference_Proceedings_Reference)) {
                        type = "4";
                    }
                    attributes[1] = type;

                    DOMNodeList *children2 = currentElement->getChildNodes();
                    const XMLSize_t nodeCount2 = children2->getLength();

                    for (XMLSize_t xx2 = 0; xx2 < nodeCount2; ++xx2) {
                        DOMNode *currentNode2 = children2->item(xx2);
                        if (currentNode2->getNodeType() &&  // is not NULL and is element
                            currentNode2->getNodeType() == DOMNode::ELEMENT_NODE) {
                            DOMElement *currentElement2 = dynamic_cast<xercesc::DOMElement *>(currentNode2);
                            if (XMLString::equals(currentElement2->getTagName(), TAG_has_author)) {
                                DOMNode *child = currentElement2->getFirstElementChild();
                                DOMElement *childElement = dynamic_cast<xercesc::DOMElement *>(child);
                                DOMNode *author_name = childElement->getFirstElementChild();
                                const XMLCh *author = author_name->getTextContent();
                                Author_name = XMLString::transcode(author);
                                long id = addToAuthors(&authors, Author_name);

                                authorsInArticle.push_back(id);

                            } else if (XMLString::equals(currentElement2->getTagName(), TAG_has_title)) {
                                const XMLCh *paper = currentElement2->getTextContent();
                                paper_title = XMLString::transcode(paper);
                                long id = addToArticles(&articles, paper_title);
                                articleID = id;
                                attributes[0] = paper_title;

                            } else if (XMLString::equals(currentElement2->getTagName(), TAG_article_of_journal)) {
                                DOMNode *child = currentElement2->getFirstElementChild();
                                DOMElement *childElement = dynamic_cast<xercesc::DOMElement *>(child);
                                DOMNode *journalName = childElement->getFirstElementChild();
                                const XMLCh *journal = journalName->getTextContent();
                                journal_name = XMLString::transcode(journal);
                                attributes[2] = journal_name;

                            } else if (XMLString::equals(currentElement2->getTagName(), TAG_has_volume)) {
                                const XMLCh *vol = currentElement2->getTextContent();

                                volume = XMLString::transcode(vol);
                                attributes[6] = volume;

                            } else if (XMLString::equals(currentElement2->getTagName(), TAG_has_date)) {
                                DOMNode *child = currentElement2->getFirstElementChild();
                                DOMElement *childElement = dynamic_cast<xercesc::DOMElement *>(child);

                                DOMNodeList *dateElements = childElement->getChildNodes();
                                const XMLSize_t elementCount = dateElements->getLength();

                                for (XMLSize_t e = 0; e < elementCount; ++e) {
                                    DOMNode *dateEle = dateElements->item(e);
                                    if (dateEle->getNodeType() &&  // is not null and is element
                                        dateEle->getNodeType() == DOMNode::ELEMENT_NODE) {
                                        DOMElement *currentDateElement = dynamic_cast<xercesc::DOMElement *>(dateEle);
                                        if (XMLString::equals(currentDateElement->getTagName(), TAG_year_of)) {
                                            const XMLCh *yearOf = currentDateElement->getTextContent();
                                            year = XMLString::transcode(yearOf);
                                            attributes[5] = year;

                                        } else if (XMLString::equals(currentDateElement->getTagName(), TAG_month_of)) {
                                            const XMLCh *monthOf = currentDateElement->getTextContent();
                                            month = XMLString::transcode(monthOf);
                                            attributes[4] = month;
                                        }
                                    }
                                }

                            } else if (XMLString::equals(currentElement2->getTagName(), TAG_has_web_address)) {
                                const XMLCh *web_add = currentElement2->getTextContent();
                                web_address = XMLString::transcode(web_add);
                                attributes[3] = web_address;
                            }
                        }
                    }
                }

                if (attributes->size() > 0) {
                    // articlesMap.insert({articleID, attributes});
                }
                for (int i = 0; i < authorsInArticle.size(); i++) {
                    for (int j = i + 1; j < authorsInArticle.size(); j++) {
                        edgelist.push_back(make_pair(authorsInArticle.at(i), authorsInArticle.at(j)));
                        addToEdges(&edgeMap, authorsInArticle.at(i), authorsInArticle.at(j), articleID);

                        file << authorsInArticle.at(i) << ' ' << authorsInArticle.at(j);
                    }
                }

                writeEdgesToFile();
            }
        }
    } catch (xercesc::XMLException &e) {
        char *message = xercesc::XMLString::transcode(e.getMessage());
        ostringstream errBuf;
        errBuf << "Error parsing file: " << message << flush;
        XMLString::release(&message);
    }
}

void GetConfig::writeEdgesToFile() {
    ofstream file;
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/");
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/");
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(this->graphID));
    file.open(Utils::getHomeDir() + "/.jasminegraph/tmp/" + std::to_string(this->graphID) + "/" +
              std::to_string(this->graphID));
    for (int i = 0; i < edgelist.size(); i++) {
        file << edgelist[i].first << " " << edgelist[i].second << endl;
    }

    file.close();
}

long GetConfig::addToAuthors(std::map<string, long> *map, string name) {
    long id;
    id = map->size() + 1;

    auto search = map->find(name);
    if (search != map->end()) {
        return search->second;
    }

    authorsTemp.insert({id, name});

    map->insert({name, id});
    return id;
}

long GetConfig::addToArticles(std::map<string, long> *map, string URI) {
    long id;
    id = map->size();
    auto search = map->find(URI);
    if (search != map->end()) {
        return search->second;
    }

    articlesTemp.insert({id, URI});

    map->insert({URI, id});
    return id;
}

void GetConfig::addToEdges(std::map<pair<int, int>, int> *map, long node_1, long node_2, long article_id) {
    map->insert({{node_1, node_2}, article_id});
}

std::map<std::pair<int, int>, int> GetConfig::getEdgeMap() { return edgeMap; }

std::map<long, string[7]> GetConfig::getAttributesMap() { return articlesMap; }
