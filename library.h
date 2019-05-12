#ifndef DB_LABS_LIBRARY_H
#define DB_LABS_LIBRARY_H

#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <sstream>
#include <cstring>
#include <fstream>
#include <iomanip>

namespace dbmsLib {
    class DBDate {
    private:
        static const int arrDays[13];
        friend std::ostream& operator<<(std::ostream& out, DBDate& date);
        friend std::string DateToStr(DBDate &date);
        int day, month, year;
    public:
        DBDate(std::string date);

        DBDate(int d, int m, int y);

        DBDate() : day(0), month(0), year(0) {}

        DBDate(DBDate &dat) : day(dat.day), month(dat.month), year(dat.year) {}

        int GetDay();

        int GetMonth();

        int GetYear();

        bool IsLeapYear(int year);

        int GetDaysInMonth(int m, int y);

        int DaysInCurYear();

        bool operator==(DBDate &date);

        bool operator<(DBDate &date);

        bool operator>(DBDate &date);

        bool operator<=(DBDate &date);

        bool operator>=(DBDate &date);

        bool operator!=(DBDate &date);

        DBDate &operator+=(int days);

        DBDate &operator-=(int days);

        int operator-(DBDate &date);
    };

    enum TableDataType {
        NoType,
        Int32,
        Double,
        String,
        Date
    };

    const int LENGTH = 24;
    const int DELTA = 10;

    struct ColumnDesc {
        char colName[LENGTH];
        TableDataType colType;
        int length;

        ColumnDesc() {}

        ColumnDesc(char *name, TableDataType type, int len) {
            strcpy(colName, name);
            colType = type;
            length = len;
        }
    };

    struct Strip {
        int nField;
        int *fieldWidth;
    };

    enum Condition {
        Undefined, Equal, NotEqual, Less, Greater, LessOrEqual, GreaterOrEqual
    };

    typedef std::map<std::string, void *> Row;
    typedef std::map<std::string, ColumnDesc> Header;

    void* GetValue(std::string value, std::string columnName, Header hdr);

    class DBTable {
    public:
        DBTable() {}

        DBTable(std::string tabName);

        DBTable(std::string tabName, Header hdr, std::string primKey);

        virtual ~DBTable() {}

        virtual Header GetHeader() = 0;

        virtual void SetHeader(Header &hdr) = 0;

        virtual int GetSize() = 0;

        virtual Row GetRow(int index) = 0;

        virtual std::vector<int> IndexOfRecord(void *keyValue, std::string keyColumnName) = 0;

        virtual Row operator[](int index) = 0;

        virtual void ReadDBTable(std::string fileName) = 0;

        virtual std::string valueToString(Row &row, std::string columnName) = 0;

        virtual void WriteDBTable(std::string fileName) = 0;

        virtual void PrintTable(int screenWidth) = 0;

        virtual void SetTableName(std::string tName) = 0;

        virtual void SetPrimaryKey(std::string key) = 0;

        virtual std::string GetTableName() = 0;

        virtual std::string GetPrimaryKey() = 0;

        virtual void SetFileName(std::string path) = 0;

        virtual std::string GetFileName() = 0;

        virtual Row CreateRow() = 0;

        virtual void AddRow(Row row, int index) = 0;

        virtual void DeleteRow(int index)=0;

        virtual DBTable *SelfRows(std::string colName, Condition cond, void *value) = 0;
    };

    class DBTableTxt : public DBTable {
    private:
        Header columnHeaders;
        std::string primaryKey;
        std::vector<Row> data;
        std::string tableName;
        std::string fileName;

        void CreateTableMaket(Strip *&strips, int &nStrips, int screenWidth);

    public:
        DBTableTxt() {}

        DBTableTxt(std::string tabName) : tableName(tabName) {}

        DBTableTxt(std::string tabName, Header hdr, std::string primKey) : tableName(tabName), columnHeaders(hdr),
                                                                           primaryKey(primKey) {}

        ~DBTableTxt() {}

        std::string valueToString(Row &row, std::string columnName);

        Header GetHeader();

        void SetHeader(Header &hdr);

        void ReadDBTable(std::string fileName);

        void WriteTableBin(std::string fileName);

        void WriteDBTable(std::string fileName);

        void PrintTable(int screenWidth);

        void SetTableName(std::string tName);

        void SetPrimaryKey(std::string key);

        std::string GetTableName();

        std::string GetPrimaryKey();

        void SetFileName(std::string path);

        std::string GetFileName();

        Row GetRow(int index);

        Row operator[](int index);

        Row CreateRow();

        void AddRow(Row row, int index);

        void DeleteRow(int index);

        void DBTableClear();

        int GetSize();

        DBTable *SelfRows(std::string colName, Condition cond, void *value);

        std::vector<int> IndexOfRecord(void *keyValue, std::string keyColumnName);
    };

    class DBTableBin : public DBTable {
    private:
        char tabName[LENGTH];
        char primaryKey[LENGTH];
        char filName[80];
        int nColumn;
        ColumnDesc *header;
        int nRows;
        char **data;
        int maxRows;

        int RowLength();

        int DataBegin();

        int FieldPosition(std::string colName);

        int FieldLength(std::string colName);

        void ResizeData(int deltaRows);

        void CreateTableMaket(Strip *&strips, int &nStrips, int screenWidth);

    public:
        DBTableBin() {}

        //DBTableBin(std::string tName);
        DBTableBin(std::string tName, Header hdr, std::string primKey);

        ~DBTableBin() {}

        int GetSize() { return nRows; }

        void SetTableName(std::string tName);

        Header GetHeader();

        void SetHeader(Header &hdr);

        std::string valueToString(Row &row, std::string columnName);

        Row operator[](int index);

        Row GetRow(int index);

        void ReadDBTable(std::string fileName);

        void WriteDBTable(std::string fileName);

        void PrintTable(int screenWidth);

        void SetPrimaryKey(std::string key);

        std::string GetTableName();

        std::string GetPrimaryKey();

        void SetFileName(std::string path);

        std::string GetFileName();

        Row CreateRow();

        void AddRow(Row row, int index);

        void DeleteRow(int index);

        DBTable *SelfRows(std::string colName, Condition cond, void *value);

        std::vector<int> IndexOfRecord(void *keyValue, std::string keyColumnName);
    };

    struct Relation {
        std::string relationName;
        std::string parentTable;
        std::string parentPrimaryKey;
        std::string childTable;
        std::string childSecondaryKey;
    };

    void PrintRow(Row row, DBTable *tab);

//-------------------------класс DBTableSet---------------------
    class DBTableSet {
    private:
        std::string dbName;
        std::map<std::string, DBTable *> db;
    public:
        DBTableSet() {};

        DBTableSet(std::string name);

        int ReadDB();

        void PrintDB(int numcol);

        void WriteDB();

        std::string GetDBName() { return dbName; }

        DBTable *operator[](std::string tableName);

        Relation GetRelation(std::string relationName);

        Row ParentRow(Relation &relation, Row &childRow);

        DBTable *ChildRows(Relation &relation, Row &parentRow);
    };
}

#endif //DB_LABS_LIBRARY_H