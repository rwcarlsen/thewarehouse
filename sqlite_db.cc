
#include "sqlite_db.h"
#include "sqlite/sqlite3.h"

#include <fstream>

class Error : public std::exception {
 public:
  Error(std::string  msg) : msg_(msg) {};
  virtual const char* what() const throw() { return msg_.c_str(); };
  std::string msg() const { return msg_; }
  virtual ~Error() throw() {}

 protected:
  std::string msg_;
};

SqlStatement::SqlStatement(sqlite3* db, std::string zSql)
    : db_(db),
      zSql_(zSql),
      stmt_(NULL) {
  Must(sqlite3_prepare_v2(db_, zSql.c_str(), -1, &stmt_, NULL));
}

SqlStatement::~SqlStatement() {
  Must(sqlite3_finalize(stmt_));
}

void SqlStatement::Exec() {
  Must(sqlite3_step(stmt_));
  Reset();
}

void SqlStatement::Reset() {
  Must(sqlite3_reset(stmt_));
}

bool SqlStatement::Step() {
  int status = sqlite3_step(stmt_);
  if (status == SQLITE_ROW) {
    return true;
  }
  Must(status);
  return false;
}

int SqlStatement::GetInt(int col) {
  return sqlite3_column_int(stmt_, col);
}

double SqlStatement::GetDouble(int col) {
  return sqlite3_column_double(stmt_, col);
}

char* SqlStatement::GetText(int col, int* n) {
  char* v = const_cast<char*>(reinterpret_cast<const char *>(
                                  sqlite3_column_text(stmt_, col)));
  if (n != NULL) {
    *n = sqlite3_column_bytes(stmt_, col);
  }
  return v;
}

void SqlStatement::BindInt(int i, int val) {
  Must(sqlite3_bind_int(stmt_, i, val));
}

void SqlStatement::BindDouble(int i, double val) {
  Must(sqlite3_bind_double(stmt_, i, val));
}

void SqlStatement::BindText(int i, const char* val) {
  Must(sqlite3_bind_text(stmt_, i, val, -1, SQLITE_TRANSIENT));
}

void SqlStatement::Must(int status) {
  if (status != SQLITE_OK && status != SQLITE_DONE && status != SQLITE_ROW) {
    std::string err = sqlite3_errmsg(db_);
    throw Error("SQL error [" + zSql_ + "]: " + err);
  }
}

SqliteDb::SqliteDb(std::string path)
    : db_(NULL),
      isOpen_(false),
      path_(path) {}

SqliteDb::~SqliteDb() {}

void SqliteDb::close() {
  if (isOpen_) {
    if (sqlite3_close(db_) == SQLITE_OK) {
      isOpen_ = false;
    }
  }
}

void SqliteDb::open() {
  if (isOpen_) {
    return;
  }

  if (sqlite3_open(path_.c_str(), &db_) == SQLITE_OK) {
    isOpen_ = true;
  } else {
    sqlite3_close(db_);
    throw Error("Unable to create/open database " + path_);
  }
}

SqlStatement::Ptr SqliteDb::Prepare(std::string sql) {
  open();
  return SqlStatement::Ptr(new SqlStatement(db_, sql));
}

void SqliteDb::Execute(std::string sql) {
  open();

  sqlite3_stmt* statement;
  int result =
    sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement, NULL);
  if (result != SQLITE_OK) {
    std::string error = sqlite3_errmsg(db_);
    throw Error("SQL error: " + sql + " " + error);
  }

  result = sqlite3_step(statement);
  if (result != SQLITE_DONE && result != SQLITE_ROW && result != SQLITE_OK) {
    std::string error = sqlite3_errmsg(db_);
    throw Error("SQL error: " + sql + " " + error);
  }

  sqlite3_finalize(statement);
}

