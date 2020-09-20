#include <cstring>
#include <fstream>
#include <set>
#include <string>

#ifndef PROPERTY_LINK
#define PROPERTY_LINK

class PropertyLink {
   private:
    unsigned int blockAddress; // contains the address of the first element in the list
    unsigned int nextPropAddress;
    
   public:
    std::string name;
    char *value = NULL;

    static const unsigned long MAX_NAME_SIZE;  // Size of a property name in bytes
    static const unsigned long MAX_VALUE_SIZE;  // Size of a property value in bytes
    static const unsigned long PROPERTY_BLOCK_SIZE;
    static unsigned int nextPropertyIndex; // Next available property block index // unless open in wipe data mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    static std::string DB_PATH;
    static std::fstream *propertiesDB;

    PropertyLink(unsigned int);
    PropertyLink();
    bool isEmpty();
    
    unsigned int insert(std::string, char[]);
    PropertyLink* next();

};

#endif