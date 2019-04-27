#include "library.h"
#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <sstream>
#include <cstring>
#include <fstream>
#include <iomanip>

const int DBDate::arrDays[13] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
std::ostream& operator<<(std::ostream& out, DBDate& date)
{
    out << date.GetDay() << '.' << date.GetMonth() << '.' << date.GetYear();
    return out;
}
std::string DateToStr(DBDate &date)
{
    std::stringstream ss;
    ss << date.GetDay() << '.' << date.GetMonth() << '.' << date.GetYear();
    std::string s;
    ss >> s;
    return s;
}
DBDate::DBDate(std::string date)
{
    std::stringstream ss;
    char c;
    ss << date;
    ss >> day >> c >> month >> c >> year;
}
DBDate::DBDate(int d, int m, int y)
{
    day = d; month = m; year = y;
}
int DBDate::GetDay()
{
    return day;
}
int DBDate::GetMonth()
{
    return month;
}
int DBDate::GetYear()
{
    return year;
}
bool DBDate::IsLeapYear(int year)
{
    return !(year%4 || ( year%100 == 0 && year%400));
}
int DBDate::GetDaysInMonth(int month, int year)
{
    if(IsLeapYear(year) && month == 2)
        return arrDays[month]+1;
    else
        return arrDays[month];
}
int DBDate::DaysInCurYear()
{
    int days = 0;
    for(int i = 1; i < month; i++)
        days += arrDays[i];
    if(IsLeapYear(year) && month >= 2)
    {
        if(month > 2)
            days++;
        else if(month == 2 && day == 29)
            days++;
    }
    days += day;
    return days;
}
bool DBDate::operator==(DBDate& date)
{
    return (date.day == day && date.month == month && date.year == year);
}
bool DBDate::operator<(DBDate& date)
{
    if(year == date.year)
    {
        if(month == date.month)
            return day < date.day;
        return month < date.month;
    }
    return year < date.year;
}
bool DBDate::operator>(DBDate& date)
{
    if(year == date.year)
    {
        if(month == date.month)
            return day > date.day;
        return month > date.month;
    }
    return year > date.year;
}
bool DBDate::operator<=(DBDate& date)
{
    if(year == date.year)
    {
        if(month == date.month)
            return day <= date.day;
        return month < date.month;
    }
    return year < date.year;
}
bool DBDate::operator>=(DBDate& date)
{
    if(year == date.year)
    {
        if(month == date.month)
            return day >= date.day;
        return month > date.month;
    }
    return year > date.year;
}
bool DBDate::operator!=(DBDate& date)
{
    return day != date.day || month != date.month || year != date.year;
}
DBDate& DBDate::operator+=(int days)
{
    while(days != 0)
    {
        if(day == arrDays[month])
        {
            if(month == 12)
            {
                year++;
                month = 1;
            }
            else
            {
                if(IsLeapYear(year) && month == 2)
                {
                    if(--days == 0)
                    {
                        day = 29;
                        break;
                    }
                }
                month++;
            }
            day = 1;
        }
        else
            day++;
        days--;
    }
    return *this;
}
DBDate& DBDate::operator-=(int days)
{
    while(days != 0)
    {
        if(day == 1)
        {
            if(month == 1)
            {
                year--;
                month = 12;
                day = arrDays[month];
            }
            else
            {
                if(--month == 2 && IsLeapYear(year))
                    day = 29;
                else
                    day = arrDays[month];
            }
        }
        else
            day--;
        days--;
    }
    return *this;
}
int DBDate::operator-(DBDate& date)
{
    int days = 0;
    DBDate tmp, tmp1;
    bool flag = 1;
    if(date < *this)
    {
        tmp = date;
        tmp1 = *this;
    }
    else
    {
        tmp = *this;
        tmp1 = date;
        flag = 0;
    }
    while(tmp != tmp1)
    {
        if(tmp.day == arrDays[tmp.month])
        {
            if(tmp.month == 12)
            {
                tmp.month = 1;
                tmp.year++;
            }
            else
            {
                if(IsLeapYear(tmp.year) && tmp.month == 2)
                    days++;
                tmp.month++;
            }
            tmp.day = 1;
        }
        else
            tmp.day++;
        days++;
    }
    if(flag)
        return days;
    else
        return -days;
}

void* GetValue(std::string value, std::string columnName, Header hdr)
{
    switch(hdr[columnName].colType)
    {
        case Int32:
            return (int *)(new int(stoi(value)));
        case String:
            return (std::string*)(new std::string(value));
        case Double:
            return (double *)(new double(stod(value)));
        case Date:
            return (DBDate *)(new DBDate(value));
        case NoType:
            return (std::string *)(new std::string(value));
        default:
            return 0;
    }
}

std::string DBTableTxt::valueToString(Row& row, std::string columnName)
{
    std::stringstream ss;
    void * ptr = row[columnName];
    switch (columnHeaders[columnName].colType) {
        case String:
            return *((std::string *) ptr);
        case Int32:
            ss << *((int *) ptr);
            return ss.str();
        case Double:
            ss << *((double *) ptr);
            return ss.str();
        case Date:
            ss << *((DBDate *) ptr);
            return ss.str();
        default:
            return *((std::string *) ptr);
    }

}

void DBTableTxt::SetHeader(Header& hdr)
{
    columnHeaders = hdr;
}

void DBTableTxt::SetTableName(std::string tName) {
    tableName = tName;
}

void DBTableTxt::SetPrimaryKey(std::string key) {
    primaryKey = key;
}

void DBTableTxt::SetFileName(std::string path) {
    fileName = path + tableName + ".txt";
}

std::string DBTableTxt::GetFileName() {
    return fileName;
}

std::string DBTableTxt::GetTableName() {
    return tableName;
}

std::string DBTableTxt::GetPrimaryKey() {
    return primaryKey;
}

Row DBTableTxt::GetRow(int index) {
    return data[index];
}

void DBTableTxt::DBTableClear() {
    data.clear();
    columnHeaders.clear();
    tableName = "";
    primaryKey = "";
    fileName = "";
}

int DBTableTxt::GetSize() {
    return data.size();
}

void DBTableTxt::ReadDBTable(std::string fileName)
{
    std::stringstream ss;
    std::ifstream in(fileName);
    if( !in.is_open() )
    {
        std::cout << "Error while opening file" << std::endl;
        return;
    }
    char buf[1024];
    char delims[] = "|\n\r";
    in.getline(buf, 100);
    char *token, *next_token;
    next_token = buf;
    tableName = strtok_r(next_token, delims, &next_token);
    primaryKey = strtok_r(next_token, delims, &next_token);
    columnHeaders.clear();
    in.getline(buf, 200);
    next_token = buf;
    std::vector<ColumnDesc> stdArray;
    ColumnDesc colHdr;

    while(token = strtok_r(next_token, delims, &next_token))
    {
        strcpy(colHdr.colName, token);
        token = strtok_r(next_token, delims, &next_token);
        if(strcmp(token, "Int32") == 0)
            colHdr.colType = Int32;
        else if(strcmp(token, "String") == 0)
            colHdr.colType = String;
        else if(strcmp(token, "Double") == 0)
            colHdr.colType = Double;
        else if(strcmp(token, "DBDate") == 0)
            colHdr.colType = Date;
        else
            colHdr.colType = NoType;
        token = strtok_r(next_token, delims, &next_token);
        if(token)
            colHdr.length = atoi(token);

        stdArray.push_back(colHdr);
    }
    Header hdr;
    for(unsigned int j = 0; j < stdArray.size(); j++)
    {
        hdr[stdArray[j].colName] = stdArray[j];
    }
    SetHeader(hdr);
    data.clear();
    while(in.getline(buf, 200))
    {
        Row row = *(new Row());
        int j = 0;
        token = strtok_r(buf, delims, &next_token);
        while(token)
        {
            std::string value(token);
            row[stdArray[j].colName] = GetValue(value, stdArray[j].colName, columnHeaders);
            j++;
            token = strtok_r(next_token, delims, &next_token);
        }
        data.push_back(row);
    }
}

void DBTableTxt::WriteDBTable(std::string fileName) {
    std::ofstream out(fileName);
    out << tableName << '|' << primaryKey << std::endl;
    std::string typeNames[] ={
            "NoType",
            "Int32",
            "Double",
            "String",
            "Date"
    };
    Header::iterator iter;
    iter = columnHeaders.begin();
    while ( iter != columnHeaders.end() )
    {
        out << iter->first << '|' << typeNames[iter->second.colType] << '|' << iter->second.length << '|';
        iter++;
    }
    out.seekp(out.tellp() - (long)1);
    out << std::endl;
    for(int i = 0; i < data.size(); i++)
    {
        iter = columnHeaders.begin();
        while ( iter != columnHeaders.end() )
        {
            out << valueToString(data[i], iter->first) << '|';
            iter++;
        }
        out.seekp(out.tellp() - (long)1);
        out << std::endl;
    }
    out.close();
}
void DBTableTxt::PrintTable(int screenWidth)
{
    Strip* strips;
    int nStrips;
    int k = 0;
    std::string typeNames[] ={
            "NoType",
            "Int32",
            "Double",
            "String",
            "Date"
    };
    Header::iterator iter, iterWhile, iter1;
    std::cout << "Table " << tableName << std::endl;
    CreateTableMaket(strips, nStrips, screenWidth);
    iter = columnHeaders.begin();
    iterWhile = iter;
    for(int i = 0; i < nStrips; i++)
    {
        std::cout << std::setw(screenWidth+1) << std::setfill('=') << '\n' << std::flush;
        for(int j = 0; j < strips[i].nField; j++)
        {
            std::cout << std::setfill(' ') << std::setw(strips[i].fieldWidth[j]) << iterWhile->second.colName;
            iterWhile++;
        }
        std::cout << std::endl;
        iterWhile = iter;
        for(int j = 0; j < strips[i].nField; j++)
        {
            std::cout << std::setfill(' ') << std::setw(strips[i].fieldWidth[j]) << typeNames[iterWhile->second.colType];
            iterWhile++;
        }
        std::cout << std::endl << std::setw(screenWidth+1) << std::setfill('-') << '\n' << std::flush;

        for(int j = 0; j < data.size(); j++)
        {
            iter1 = iter;
            for(k = 0; k < strips[i].nField; k++)
            {
               std::cout << std::setw(strips[i].fieldWidth[k]) << std::setfill(' ') << valueToString(data[j],iter1->second.colName);
               iter1++;
            }
            std::cout << std::endl;
        }
        std::cout << std::setw(screenWidth+1) << std::setfill('=') << '\n' << std::endl;
        iter = iterWhile;
    }
    for(int i = 0; i < nStrips; i++)
    {
        delete[](strips[i].fieldWidth);
    }
    delete[] strips;
}

Row DBTableTxt::operator[](int ind)
{
    return data[ind];
}
Row DBTableTxt::CreateRow()
{
    Header::iterator iter;
    std::string s;
    Row row;
    iter = columnHeaders.begin();
    while ( iter != columnHeaders.end() )
    {
        std::cout << "Введите столбец " << iter->first << ' ' << std::endl;
        std::cin >> s;
        std::getline(std::cin, s);
        row[iter->first] = GetValue(s,iter->first, columnHeaders);
        iter++;
    }
    return row;
}
void DBTableTxt::AddRow(Row row, int index)
{
    std::vector<Row>::iterator iter;
    iter = data.begin();
    for(int i = 0; i < index - 1; i++)
        iter++;
    data.insert(iter, row);
}

void DBTableTxt::DeleteRow(int index) {
    std::vector<Row>::iterator iter;
    iter = data.begin();
    for(int i = 0; i < index - 1; i++)
        iter++;
    data.erase(iter);
}

Header DBTableTxt::GetHeader() {
    return columnHeaders;
}


void DBTableTxt::WriteTableBin(std::string fileName)
{
    std::ofstream out(fileName, std::ios::binary);
    if( !out.is_open() )
    {
        std::cout << "Error while opening file" << std::endl;
        return;
    }
    int len = LENGTH;
    out.write( (char *)&len, sizeof(int) );
    char tabName[LENGTH];
    strcpy( tabName, tableName.c_str() );
    out.write(tabName, LENGTH);
    out.write( (char *)&len, sizeof(int) );
    char primKey[LENGTH];
    strcpy( primKey, primaryKey.c_str() );
    out.write(primKey, LENGTH);
    int nColumn = columnHeaders.size();
    int nRows = data.size();
    ColumnDesc* header = new ColumnDesc[nColumn];
    Header::iterator iter;
    iter = columnHeaders.begin();
    for(int i = 0; i < nColumn; i++)
    {
        strcpy(header[i].colName, iter->second.colName);
        header[i].length = iter->second.length;
        header[i].colType = iter->second.colType;
        iter++;
    }
    out.write( (char *)&nColumn, sizeof(int) );
    out.write( (char *)header, nColumn*sizeof(ColumnDesc) );
    out.write( (char *)&nRows, sizeof(int) );
    int i = 0;
    void * ptr;
    for(i = 0; i < nRows; i++)
    {
        iter = columnHeaders.begin();
        while(iter != columnHeaders.end() )
        {
            ptr = data[i][iter->first];
            switch (iter->second.colType)
            {
                case Int32:
                    out.write((char *)ptr, sizeof(int));
                    break;
                case String:
                    out.write( ((std::string *)ptr)->c_str(),  iter->second.length );
                    break;
                case Double:
                    out.write( (char *)ptr, sizeof(double) );
                    break;
                case Date:
                    out.write( (char *)ptr, sizeof(DBDate) );
            }
            iter++;
        }
    }
    out.close();
}

void DBTableTxt::CreateTableMaket(Strip*& strips, int& nStrips, int screenWidth)
{
    Header::iterator iter;
    iter = columnHeaders.begin();
    int sum = 0, j;
    while(iter != columnHeaders.end())
    {
        sum += iter->second.length+3;
        iter++;
    }
    nStrips = sum/screenWidth;
    strips = new Strip[++nStrips];
    iter = columnHeaders.begin();
    int len;
    for (int i = 0; i < nStrips; i++)
    {
        strips[i].nField = 0;
        strips[i].fieldWidth = new int[columnHeaders.size()];
        sum = 0, j = 0;
        do {
            if(iter->first.length() > iter->second.length)
                len = iter->first.length() + 3;
            else
                len = iter->second.length + 3;
            strips[i].fieldWidth[j] = len;
            j++;
            sum += len;
            iter++;
            strips[i].nField++;
        } while( sum + len <= screenWidth && iter != columnHeaders.end() );
    }
}

DBTableBin::DBTableBin(std::string tName, Header hdr, std::string primKey) {
    strcpy(tabName, tName.c_str());
    SetHeader(hdr);
    strcpy(primaryKey, primKey.c_str());
    nRows = 0;
}

Header DBTableBin::GetHeader() {
    Header hdr;
    for(int i = 0; i < nColumn; i++)
    {
        hdr[header[i].colName] = header[i];
    }
    return hdr;
}

void DBTableBin::SetHeader(Header &hdr) {

    Header::iterator iter;
    nColumn = hdr.size();
    iter = hdr.begin();
    header = new ColumnDesc[hdr.size()];
    for(int i = 0; i < nColumn; i++)
    {
        strcpy(header[i].colName, iter->second.colName);
        header[i].length = iter->second.length;
        header[i].colType = iter->second.colType;
        iter++;
    }
}

void DBTableBin::ReadDBTable(std::string fileName) {
    std::ifstream in(fileName, std::ios::binary);
    if(!in.is_open())
    {
        std::cout << "Error while opening file" << std::endl;
        return;
    }
    in.seekg(sizeof(int));
    in.read(tabName, LENGTH);
    in.seekg(sizeof(int), std::ios_base::cur);
    in.read(primaryKey, LENGTH);
    in.read( (char *)&nColumn, sizeof(int) );
    header = new ColumnDesc[nColumn];
    in.read( (char *)header, nColumn*sizeof(ColumnDesc) );
    in.read( (char *)&nRows, sizeof(int) );
    maxRows = nRows + DELTA;
    data = new char*[maxRows];
    int i, j;
    char * buf;
    std::string* bufstr;
    for(i = 0; i < nRows; i++)
    {
        data[i] = new char[sizeof(Row)];
        Row* row = new Row;
        for(j = 0; j < nColumn; j++)
        {
            switch (header[j].colType)
            {
                case Int32:
                    buf = new char[sizeof(int)];
                    in.read(buf, sizeof(int) );
                    (*row)[header[j].colName] = buf;
                    break;
                case String:
                    buf = new char[header[j].length];
                    in.read(buf, header[j].length);
                    bufstr = new std::string(buf);
                    (*row)[header[j].colName] = bufstr;
                    break;
                case Double:
                    buf = new char[sizeof(double)];
                    in.read(buf, sizeof(double));
                    (*row)[header[j].colName] = buf;
                    break;
                case Date:
                    buf = new char[sizeof(DBDate)];
                    in.read(buf, sizeof(DBDate));
                    (*row)[header[j].colName] = buf;
                    break;
            }
        }
        data[i] = (char *)row;
    }
    in.close();
}

void DBTableBin::WriteDBTable(std::string fileName) {
    std::ofstream out(fileName, std::ios::binary);
    if( !out.is_open() )
    {
        std::cout << "Error while opening file" << std::endl;
        return;
    }
    int len = LENGTH;
    out.write( (char *)&len, sizeof(int) );
    out.write(tabName, LENGTH);
    out.write( (char *)&len, sizeof(int) );
    out.write(primaryKey, LENGTH);
    out.write( (char *)&nColumn, sizeof(int) );
    out.write( (char *)header, nColumn*sizeof(ColumnDesc) );
    out.write( (char *)&nRows, sizeof(int) );
    int i = 0, j;
    Row *row; void * ptr;
    for(i = 0; i < nRows; i++)
    {
        row = (Row *)data[i];
        for(j = 0; j < nColumn; j++)
        {
            ptr = (*row)[header[j].colName];
            switch (header[j].colType)
            {
                case Int32:
                    out.write( (char *)ptr, sizeof(int) );
                    break;
                case String:
                    out.write(((std::string *)ptr)->c_str(), header[j].length);
                    break;
                case Double:
                    out.write( (char *)ptr, sizeof(double) );
                    break;
                case Date:
                    out.write( (char *)ptr, sizeof(DBDate) );
                    break;
            }
        }
    }
    out.close();
}

Row DBTableBin::operator[](int index) {
    Row *row = (Row *)data[index];
    return *row;
}

Row DBTableBin::GetRow(int index) {
    Row *row = (Row *)data[index];
    return *row;
}

Row DBTableBin::CreateRow() {
    int i;
    Row row;
    std::string s;
    Header hdr = GetHeader();
    Header::iterator iter;
    iter = hdr.begin();
    while (iter != hdr.end() )
    {
        std::cout << "Введите столбец " << iter->first << ": ";
        std::getline(std::cin, s);
        row[iter->first] = GetValue(s,iter->first, hdr);
        iter++;
    }
    return row;
}

void DBTableBin::SetTableName(std::string tName) {
    strcpy( tabName, tName.c_str() );
}

void DBTableBin::ResizeData(int deltaRows) {
    maxRows += deltaRows;
    char** dataTmp = new char*[maxRows];
    for(int i = 0; i < nRows; i++)
        dataTmp[i] = data[i];
    delete[] data;
    data = dataTmp;
}

void DBTableBin::AddRow(Row row, int index) {
    if(nRows  == maxRows )
        ResizeData(DELTA);
    if(index > nRows)
        index = nRows;
    else
    {
        Row* r = (Row *)data[index];
        Row* r1;
        for(int i = index; i < nRows; i++ )
        {
            r1 = (Row*)data[i+1];
            data[i+1] = (char *)r;
            r = r1;
        }
    }
    Row *row1 = new Row(row);
    data[index] = (char *)row1;
    nRows++;
}

std::string DBTableBin::valueToString(Row &row, std::string columnName) {
    std::stringstream ss;
    void * ptr = row[columnName];
    Header columnHeaders = GetHeader();
    switch (columnHeaders[columnName].colType) {
        case String:
            return *((std::string *) ptr);
        case Int32:
            ss << *((int *) ptr);
            return ss.str();
        case Double:
            ss << *((double *) ptr);
            return ss.str();
        case Date:
            ss << *((DBDate *) ptr);
            return ss.str();
        default:
            return *((std::string *) ptr);
    }
}

void DBTableBin::CreateTableMaket(Strip*& strips, int& nStrips, int screenWidth)
{
    Header columnHeaders = GetHeader();
    Header::iterator iter;
    iter = columnHeaders.begin();
    int sum = 0, j;
    while(iter != columnHeaders.end())
    {
        sum += iter->second.length+3;
        iter++;
    }
    nStrips = sum/screenWidth;
    strips = new Strip[++nStrips];
    iter = columnHeaders.begin();
    int len;
    for (int i = 0; i < nStrips; i++)
    {
        strips[i].nField = 0;
        strips[i].fieldWidth = new int[columnHeaders.size()];
        sum = 0, j = 0;
        do {
            if(iter->first.length() > iter->second.length)
                len = iter->first.length() + 3;
            else
                len = iter->second.length + 3;
            strips[i].fieldWidth[j] = len;
            j++;
            sum += len;
            iter++;
            strips[i].nField++;
        } while( sum + len <= screenWidth && iter != columnHeaders.end() );
    }
}

void DBTableBin::PrintTable(int screenWidth)
{
    Strip* strips;
    int nStrips;
    int k = 0;
    std::string typeNames[] ={
            "NoType",
            "Int32",
            "Double",
            "String",
            "Date"
    };
    Header columnHeaders = GetHeader();
    Header::iterator iter, iterWhile, iter1;
    std::cout << "Table " << tabName << std::endl;
    CreateTableMaket(strips, nStrips, screenWidth);
    iter = columnHeaders.begin();
    iterWhile = iter;
    for(int i = 0; i < nStrips; i++)
    {
        std::cout << std::setw(screenWidth+1) << std::setfill('=') << '\n' << std::flush;
        for(int j = 0; j < strips[i].nField; j++)
        {
            std::cout << std::setfill(' ') << std::setw(strips[i].fieldWidth[j]) << iterWhile->second.colName;
            iterWhile++;
        }
        std::cout << std::endl;
        iterWhile = iter;
        for(int j = 0; j < strips[i].nField; j++)
        {
            std::cout << std::setfill(' ') << std::setw(strips[i].fieldWidth[j]) << typeNames[iterWhile->second.colType];
            iterWhile++;
        }
        std::cout << std::endl << std::setw(screenWidth+1) << std::setfill('-') << '\n' << std::flush;

        for(int j = 0; j < nRows; j++)
        {
            iter1 = iter;
            for(k = 0; k < strips[i].nField; k++)
            {
               std::cout << std::setw(strips[i].fieldWidth[k]) << std::setfill(' ') << valueToString(*((Row *)data[j]),iter1->second.colName);
               iter1++;
            }
            std::cout << std::endl;
        }
        std::cout << std::setw(screenWidth+1) << std::setfill('=') << '\n' << std::endl;
        iter = iterWhile;
    }
    for(int i = 0; i < nStrips; i++)
    {
        delete[](strips[i].fieldWidth);
    }
    delete[] strips;
}

bool comparator(TableDataType type, void* obj1, void* obj2, Condition cond)
{
    switch (type)
    {
        case Int32:
            switch (cond)
            {
                case Equal:
                    return *((int *)obj1) == *((int *)obj2);
                case NotEqual:
                    return *((int *)obj1) != *((int *)obj2);
                case Less:
                    return *((int *)obj1) < *((int *)obj2);
                case Greater:
                    return *((int *)obj1) > *((int *)obj2);
                case LessOrEqual:
                    return *((int *)obj1) <= *((int *)obj2);
                case GreaterOrEqual:
                    return *((int *)obj1) >= *((int *)obj2);
                case Undefined:
                    return false;
            }
        case String:
            switch (cond)
            {
                case Equal:
                    return *((std::string *)obj1) == *((std::string *)obj2);
                case NotEqual:
                    return *((std::string *)obj1) != *((std::string *)obj2);
                case Less:
                    return *((std::string *)obj1) < *((std::string *)obj2);
                case Greater:
                    return *((std::string *)obj1) > *((std::string *)obj2);
                case LessOrEqual:
                    return *((std::string *)obj1) <= *((std::string *)obj2);
                case GreaterOrEqual:
                    return *((std::string *)obj1) >= *((std::string *)obj2);
                case Undefined:
                    return false;
            }
        case Double:
            switch (cond)
            {
                case Equal:
                    return *((double *)obj1) == *((double *)obj2);
                case NotEqual:
                    return *((double *)obj1) != *((double *)obj2);
                case Less:
                    return *((double *)obj1) < *((double *)obj2);
                case Greater:
                    return *((double *)obj1) > *((double *)obj2);
                case LessOrEqual:
                    return *((double *)obj1) <= *((double *)obj2);
                case GreaterOrEqual:
                    return *((double *)obj1) >= *((double *)obj2);
                case Undefined:
                    return false;
            }
        case Date:
            switch (cond)
            {
                case Equal:
                    return *((DBDate *)obj1) == *((DBDate *)obj2);
                case NotEqual:
                    return *((DBDate *)obj1) != *((DBDate *)obj2);
                case Less:
                    return *((DBDate *)obj1) < *((DBDate *)obj2);
                case Greater:
                    return *((DBDate *)obj1) > *((DBDate *)obj2);
                case LessOrEqual:
                    return *((DBDate *)obj1) <= *((DBDate *)obj2);
                case GreaterOrEqual:
                    return *((DBDate *)obj1) >= *((DBDate *)obj2);
                case Undefined:
                    return false;
            }
    }
    return false;
}

DBTable* DBTableBin::SelfRows(std::string colName, Condition cond, void *value) {
    Header hdr = GetHeader();
    std::string s1(tabName), s2(primaryKey);
    DBTable* tab = new DBTableBin(s1, hdr, s2);
    Row val; int k = 0;
    for(int i = 0; i < nRows; i++) {
        val = this->GetRow(i);
        if( comparator(hdr[colName].colType, val[colName], value, cond) ) {
            tab->AddRow(val, k);
            k++;
        }
    }
    return tab;
}

DBTable* DBTableTxt::SelfRows(std::string colName, Condition cond, void *value) {
    DBTable* tab = new DBTableTxt(tableName, columnHeaders, primaryKey);
    Row val; int k = 0;
    for(int i = 0; i < data.size(); i++) {
        val = data[i];
        if( comparator(columnHeaders[colName].colType, val[colName], value, cond) ) {
            tab->AddRow(val, k);
            k++;
        }
    }
    return tab;
}

std::vector<int> DBTableBin::IndexOfRecord(void *keyValue, std::string keyColumnName) {
    Header hdr = GetHeader();
    std::vector<int> tmp;
    Row val;
    for(int i = 0; i < nRows; i++)
    {
        val = this->GetRow(i);
        if( comparator(hdr[keyColumnName].colType, val[keyColumnName], keyValue, Equal ) )
        {
            tmp.push_back(i);
        }
    }
    return tmp;
}

std::vector<int> DBTableTxt::IndexOfRecord(void *keyValue, std::string keyColumnName) {
    std::vector<int> tmp;
    Row val;
    for(int i = 0; i < data.size(); i++)
    {
        val = data[i];
        if( comparator(columnHeaders[keyColumnName].colType, val[keyColumnName], keyValue, Equal ) )
        {
            tmp.push_back(i);
        }
    }
    return tmp;
}

void DBTableBin::SetFileName(std::string path) {
    strcpy(filName, (path+tabName+".bin").c_str() );
}

void DBTableBin::SetPrimaryKey(std::string key) {
    strcpy(primaryKey, key.c_str() );
}

std::string DBTableBin::GetTableName() {
    return std::string(tabName);
}

std::string DBTableBin::GetPrimaryKey() {
    return std::string(primaryKey);
}

std::string DBTableBin::GetFileName() {
    return std::string(filName);
}

DBTableSet::DBTableSet(std::string name) {
    dbName = name;
}

DBTable* DBTableSet::operator[](std::string tableName) {
    return db[tableName];
}

int DBTableSet::ReadDB() {
    DBTable* tab;
    std::ifstream in(dbName + "/DBTables.txt");
    if(!in.is_open())
        return 0;
    char buf[1024];
    char delims[] = "|\r\n";
    char* token,* next_token;
    in.getline(buf, 80);
    next_token = buf;
    std::string dbType = dbName.substr(dbName.size()-3, 3);
    while ( token = strtok_r(next_token, delims, &next_token) )
    {
        if(dbType == "Bin")
            tab = new DBTableBin();
        else if(dbType == "Txt")
            tab = new DBTableTxt();
        else
        {
            in.close();
            return  0;
        }
        tab->ReadDBTable(dbName + "/" + token);
        db[token] = tab;
    }
    in.close();
    return 1;
}

void DBTableSet::PrintDB(int numcol) {
    auto iter = db.begin();
    while( iter != db.end() )
    {
        iter->second->PrintTable(numcol);
        iter++;
    }
}

void DBTableSet::WriteDB() {
    std::string dbType = dbName.substr(dbName.size()-3, 3);
    std::string tabType;
    std::ofstream out(dbName + "/DBTables.txt");
    if(!out.is_open()) {
        std::cout << "Error opening file" << std::endl;
        return;
    }
    for(auto iter = db.begin(); iter != db.end(); iter++)
    {
        iter->second->WriteDBTable(dbName + '/' + iter->first);
        out << iter->first << '|';
    }
    out.seekp(out.tellp() - (long)1);
    out << std::endl;
    out.close();
}